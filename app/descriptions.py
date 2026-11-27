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
from .accounts import active_user, cancel_unsent, is_deleted
from .auth import Principal
from .config import get_settings
from .db import get_db
from .ids import UUID_PATTERN, canonical
from .messaging.outbox import deterministic_uuid
from .messaging.pending import pending_event, relay

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
    # RF9: solo un comercio verificado genera descripciones; sin comercio no hay llamada al LLM.
    merchantId: str = Field(pattern=UUID_PATTERN)


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
async def generate_description(body: DescriptionRequest, request: Request, user: Principal = Depends(active_user)):
    db = get_db()
    draft = canonical(body.draftId)
    key = {"requestedBy": user.user_id, "draftId": draft}
    now = datetime.now(UTC)

    existing = await db.descriptions.find_one(key)
    if existing and existing.get("status", "done") == "done":
        # Si una petición anterior guardó la descripción pero murió antes de copiar su mensaje al outbox,
        # se completa aquí (además del barrido del publicador).
        if existing.get("pendingEvents"):
            await relay(db, "descriptions", existing["_id"], existing["pendingEvents"])
        return _cached(existing, draft)
    if existing and existing["claimedAt"] > now - timedelta(seconds=CLAIM_SECONDS):
        raise _busy()  # otra petición la está generando ahora mismo

    # RF9 es para comercios: el comercio debe ser del usuario y estar verificado hoy (merchant.updated puede
    # revocarlo). Va antes del límite y de la reclamación para que una cuenta sin comercio no gaste cupo ni LLM.
    merchant = await db.merchants.find_one({"_id": canonical(body.merchantId), "userId": user.user_id, "verified": True})
    if merchant is None:
        raise HTTPException(status_code=403, detail="el comercio no es tuyo o no está verificado")

    # Cada descripción nueva es una llamada al LLM: se limita por usuario (RNF8).
    allowed, retry_after = await ratelimit.hit(user.user_id, scope="descriptions")
    if not allowed:
        raise HTTPException(status_code=429, detail="vas muy rápido, espera un momento", headers={"Retry-After": str(retry_after)})


    # Token propio de este intento: solo quien lo tiene puede finalizar o liberar la reclamación.
    token = str(uuid.uuid4())
    if existing:
        # Reclamación vencida (la petición anterior murió): se retoma solo si nadie más la tomó.
        request_id = existing["_id"]
        taken = await db.descriptions.update_one(
            {"_id": request_id, "status": "generating", "claimedAt": existing["claimedAt"]},
            {"$set": {"claimedAt": now, "claimToken": token}},
        )
        if taken.matched_count == 0:
            raise _busy()
    else:
        request_id = str(uuid.uuid4())
        try:
            await db.descriptions.insert_one({
                "_id": request_id, "requestedBy": user.user_id, "draftId": draft, "status": "generating",
                "claimedAt": now, "claimToken": token,
                "merchantId": canonical(body.merchantId), "createdAt": now,
            })
        except DuplicateKeyError as err:
            raise _busy() from err  # otra petición reclamó el borrador en el mismo instante

    mine = {"_id": request_id, "claimToken": token, "status": "generating"}
    settings = get_settings()
    try:
        result = await llm.complete(SYSTEM, [{"role": "user", "content": build_prompt(body, merchant)}], settings.llm_description_model, max_tokens=400)
    except llm.LLMError as err:
        await db.descriptions.delete_one(mine)  # libera solo nuestra reclamación, para reintentar
        raise HTTPException(status_code=503, detail="la IA no está disponible ahora, intenta de nuevo") from err
    text = clean(result.text)
    if not text:
        await db.descriptions.delete_one(mine)
        raise HTTPException(status_code=503, detail="la IA no devolvió una descripción")

    event = pending_event(
        # Id estable por solicitud: si el mensaje se copia dos veces al outbox (petición + barrido), es uno solo.
        event_id=deterministic_uuid(f"description.generated:{request_id}"),
        exchange="festa.ai",
        routing_key="description.generated",
        payload={
            "requestId": request_id,
            "requestedBy": user.user_id,
            "merchantId": canonical(body.merchantId),
            "draftId": draft,
            "provider": result.provider[:30],
            "model": result.model[:60],
            "inputTokens": result.input_tokens,
            "outputTokens": result.output_tokens,
            "generatedAt": now.isoformat().replace("+00:00", "Z"),
        },
        correlation_id=request.headers.get("x-correlation-id"),
    )
    # La descripción y su mensaje se guardan en la MISMA escritura (un documento = atómico): no puede quedar
    # una descripción en caché sin su description.generated.
    finished = await db.descriptions.update_one(
        mine,
        {
            "$set": {"status": "done", "text": text, "provider": result.provider, "model": result.model,
                     "inputTokens": result.input_tokens, "outputTokens": result.output_tokens},
            "$unset": {"claimedAt": "", "claimToken": ""},
            "$push": {"pendingEvents": event},
        },
    )
    if finished.matched_count == 0:
        # Nuestra reclamación venció y otra petición ya la retomó: no se sobrescribe su resultado.
        raise _busy()
    await relay(db, "descriptions", request_id, [event])
    if await is_deleted(db, user.user_id):
        # La cuenta se borró mientras se generaba: account.deleted pudo correr antes de esta escritura,
        # así que se deshace aquí (mismo efecto que su borrado) y no se publica el id de la persona.
        await db.descriptions.update_one(
            {"_id": request_id}, {"$set": {"requestedBy": None}, "$unset": {"pendingEvents": ""}}
        )
        await cancel_unsent(db, user.user_id, [event["eventId"]])
        raise HTTPException(status_code=403, detail="la cuenta fue eliminada")
    return DescriptionResponse(draftId=draft, description=text, cached=False, provider=result.provider, model=result.model)
