"""RF9: descripción atractiva de un evento a partir de datos básicos, redactada por IA Generativa.

RNF8 (1): se genera UNA sola vez por borrador (draftId) y se guarda; volver a pedirla devuelve la
guardada sin costo. Para otra versión, la app crea otro borrador.

Un borrador se reclama (status "generating") antes de llamar al LLM: dos peticiones simultáneas no
pagan dos generaciones. Si la reclamación vence (la petición murió a mitad), otra la retoma.
"""
import uuid
from datetime import UTC, datetime, timedelta
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field
from pymongo.errors import DuplicateKeyError

from . import llm, ratelimit
from .auth import Principal, current_user
from .config import get_settings
from .db import get_db
from .ids import UUID_PATTERN, canonical
from .messaging.outbox import enqueue

router = APIRouter(prefix="/ai")

# Tiempo que una petición conserva el borrador mientras genera; después otra puede retomarlo.
CLAIM_SECONDS = 90

SYSTEM = (
    "Eres redactor de Festa, una app para descubrir planes en la comunidad de la Escuela Colombiana de Ingeniería. "
    "Escribes en español de Colombia, cercano y entusiasta, sin exagerar ni inventar datos: usa solo la información "
    "que te dan (no inventes precios, horarios, artistas ni promociones). Máximo 600 caracteres, 2 o 3 frases, sin "
    "emojis en exceso, sin hashtags y sin comillas. Responde solo con el texto de la descripción."
)


class DescriptionRequest(BaseModel):
    draftId: str = Field(pattern=UUID_PATTERN, description="Id que la app asigna al borrador")
    name: str = Field(min_length=3, max_length=120)
    location: str = Field(min_length=2, max_length=200)
    activityType: str = Field(min_length=2, max_length=120)
    category: Literal["cultural", "musical", "deportivo", "gastronomico"] | None = None
    startsAt: str | None = Field(default=None, max_length=40)
    notes: str | None = Field(default=None, max_length=300)
    merchantId: str | None = Field(default=None, pattern=UUID_PATTERN)


class DescriptionResponse(BaseModel):
    draftId: str
    description: str
    cached: bool
    provider: str
    model: str


def build_prompt(req: DescriptionRequest, merchant: dict | None) -> str:
    lines = [
        "Escribe la descripción de este evento:",
        f"- nombre: {req.name}",
        f"- lugar: {req.location}",
        f"- actividad: {req.activityType}",
    ]
    if req.category:
        lines.append(f"- categoria: {req.category}")
    if req.startsAt:
        lines.append(f"- fecha: {req.startsAt}")
    if merchant:
        lines.append(f"- organizador: {merchant.get('businessName')}")
    if req.notes:
        lines.append(f"- notas del organizador: {req.notes}")
    return "\n".join(lines)


def clean(text: str) -> str:
    text = text.strip().strip('"').strip()
    return text[:2000]


def _cached(doc: dict, draft: str) -> DescriptionResponse:
    return DescriptionResponse(draftId=draft, description=doc["text"], cached=True, provider=doc["provider"], model=doc["model"])


def _busy() -> HTTPException:
    return HTTPException(status_code=409, detail="la descripción de este borrador ya se está generando, intenta en unos segundos")


@router.post("/descriptions", response_model=DescriptionResponse)
async def generate_description(body: DescriptionRequest, request: Request, user: Principal = Depends(current_user)):
    db = get_db()
    draft = canonical(body.draftId)
    key = {"requestedBy": user.user_id, "draftId": draft}
    now = datetime.now(UTC)

    existing = await db.descriptions.find_one(key)
    if existing and existing.get("status", "done") == "done":
        return _cached(existing, draft)
    if existing and existing["claimedAt"] > now - timedelta(seconds=CLAIM_SECONDS):
        raise _busy()  # otra petición la está generando ahora mismo

    # Cada descripción nueva es una llamada al LLM: se limita por usuario (RNF8).
    allowed, retry_after = await ratelimit.hit(user.user_id, scope="descriptions")
    if not allowed:
        raise HTTPException(status_code=429, detail="vas muy rápido, espera un momento", headers={"Retry-After": str(retry_after)})

    merchant = None
    if body.merchantId:
        merchant = await db.merchants.find_one({"_id": canonical(body.merchantId), "userId": user.user_id})
        if merchant is None:
            raise HTTPException(status_code=403, detail="el comercio no es tuyo o no está verificado")

    if existing:
        # Reclamación vencida (la petición anterior murió): se retoma solo si nadie más la tomó.
        request_id = existing["_id"]
        taken = await db.descriptions.update_one(
            {"_id": request_id, "status": "generating", "claimedAt": existing["claimedAt"]},
            {"$set": {"claimedAt": now}},
        )
        if taken.matched_count == 0:
            raise _busy()
    else:
        request_id = str(uuid.uuid4())
        try:
            await db.descriptions.insert_one({
                "_id": request_id, "requestedBy": user.user_id, "draftId": draft, "status": "generating",
                "claimedAt": now, "merchantId": canonical(body.merchantId) if body.merchantId else None, "createdAt": now,
            })
        except DuplicateKeyError as err:
            raise _busy() from err  # otra petición reclamó el borrador en el mismo instante

    settings = get_settings()
    try:
        result = await llm.complete(SYSTEM, [{"role": "user", "content": build_prompt(body, merchant)}], settings.llm_description_model, max_tokens=400)
    except llm.LLMError as err:
        await db.descriptions.delete_one({"_id": request_id, "status": "generating"})  # libera para reintentar
        raise HTTPException(status_code=503, detail="la IA no está disponible ahora, intenta de nuevo") from err
    text = clean(result.text)
    if not text:
        await db.descriptions.delete_one({"_id": request_id, "status": "generating"})
        raise HTTPException(status_code=503, detail="la IA no devolvió una descripción")

    await db.descriptions.update_one(
        {"_id": request_id},
        {
            "$set": {"status": "done", "text": text, "provider": result.provider, "model": result.model,
                     "inputTokens": result.input_tokens, "outputTokens": result.output_tokens},
            "$unset": {"claimedAt": ""},
        },
    )
    await enqueue(
        db,
        exchange="festa.ai",
        routing_key="description.generated",
        payload={
            "requestId": request_id,
            "requestedBy": user.user_id,
            **({"merchantId": canonical(body.merchantId)} if body.merchantId else {}),
            "draftId": draft,
            "provider": result.provider[:30],
            "model": result.model[:60],
            "inputTokens": result.input_tokens,
            "outputTokens": result.output_tokens,
            "generatedAt": now.isoformat().replace("+00:00", "Z"),
        },
        correlation_id=request.headers.get("x-correlation-id"),
    )
    return DescriptionResponse(draftId=draft, description=text, cached=False, provider=result.provider, model=result.model)
