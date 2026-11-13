"""RF9: descripción atractiva de un evento a partir de datos básicos, redactada por IA Generativa.

RNF8 (1): se genera UNA sola vez por borrador (draftId) y se guarda; volver a pedirla devuelve la
guardada sin costo. Para otra versión, la app crea otro borrador.
"""
import uuid
from datetime import UTC, datetime
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from . import llm
from .auth import Principal, current_user
from .config import get_settings
from .db import get_db
from .messaging.outbox import enqueue

router = APIRouter(prefix="/ai")

SYSTEM = (
    "Eres redactor de Festa, una app para descubrir planes en la comunidad de la Escuela Colombiana de Ingeniería. "
    "Escribes en español de Colombia, cercano y entusiasta, sin exagerar ni inventar datos: usa solo la información "
    "que te dan (no inventes precios, horarios, artistas ni promociones). Máximo 600 caracteres, 2 o 3 frases, sin "
    "emojis en exceso, sin hashtags y sin comillas. Responde solo con el texto de la descripción."
)


class DescriptionRequest(BaseModel):
    draftId: str = Field(pattern=r"^[0-9a-fA-F-]{36}$", description="Id que la app asigna al borrador")
    name: str = Field(min_length=3, max_length=120)
    location: str = Field(min_length=2, max_length=200)
    activityType: str = Field(min_length=2, max_length=120)
    category: Literal["cultural", "musical", "deportivo", "gastronomico"] | None = None
    startsAt: str | None = Field(default=None, max_length=40)
    notes: str | None = Field(default=None, max_length=300)
    merchantId: str | None = Field(default=None, pattern=r"^[0-9a-fA-F-]{36}$")


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


@router.post("/descriptions", response_model=DescriptionResponse)
async def generate_description(body: DescriptionRequest, request: Request, user: Principal = Depends(current_user)):
    db = get_db()
    existing = await db.descriptions.find_one({"requestedBy": user.user_id, "draftId": body.draftId})
    if existing:
        return DescriptionResponse(draftId=body.draftId, description=existing["text"], cached=True, provider=existing["provider"], model=existing["model"])

    merchant = None
    if body.merchantId:
        merchant = await db.merchants.find_one({"_id": body.merchantId, "userId": user.user_id})
        if merchant is None:
            raise HTTPException(status_code=403, detail="el comercio no es tuyo o no está verificado")

    settings = get_settings()
    try:
        result = await llm.complete(SYSTEM, [{"role": "user", "content": build_prompt(body, merchant)}], settings.llm_description_model, max_tokens=400)
    except llm.LLMError as err:
        raise HTTPException(status_code=503, detail="la IA no está disponible ahora, intenta de nuevo") from err
    text = clean(result.text)
    if not text:
        raise HTTPException(status_code=503, detail="la IA no devolvió una descripción")

    now = datetime.now(UTC)
    request_id = str(uuid.uuid4())
    insert = await db.descriptions.update_one(
        {"requestedBy": user.user_id, "draftId": body.draftId},
        {"$setOnInsert": {"_id": request_id, "text": text, "provider": result.provider, "model": result.model,
                          "inputTokens": result.input_tokens, "outputTokens": result.output_tokens,
                          "merchantId": body.merchantId, "createdAt": now}},
        upsert=True,
    )
    if insert.upserted_id is None:
        # Otra petición simultánea con el mismo draftId ganó: se devuelve la guardada.
        saved = await db.descriptions.find_one({"requestedBy": user.user_id, "draftId": body.draftId})
        return DescriptionResponse(draftId=body.draftId, description=saved["text"], cached=True, provider=saved["provider"], model=saved["model"])

    await enqueue(
        db,
        exchange="festa.ai",
        routing_key="description.generated",
        payload={
            "requestId": request_id,
            "requestedBy": user.user_id,
            **({"merchantId": body.merchantId} if body.merchantId else {}),
            "draftId": body.draftId.lower(),
            "provider": result.provider[:30],
            "model": result.model[:60],
            "inputTokens": result.input_tokens,
            "outputTokens": result.output_tokens,
            "generatedAt": now.isoformat().replace("+00:00", "Z"),
        },
        correlation_id=request.headers.get("x-correlation-id"),
    )
    return DescriptionResponse(draftId=body.draftId, description=text, cached=False, provider=result.provider, model=result.model)
