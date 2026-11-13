"""RF8: recomendaciones personalizadas conversando con el usuario sobre sus intereses del momento.

En cada turno se arma una lista corta de eventos candidatos (catálogo local alimentado por RabbitMQ),
el modelo responde en lenguaje natural y elige ids SOLO de esa lista (se validan: no puede inventar
eventos). RNF8 (2): modelo de bajo costo y límite de mensajes por usuario por minuto.
RN6: las recomendaciones son sugerencias automatizadas de bajo impacto (lo dice la respuesta `disclaimer`).
"""
import json
import uuid
from datetime import UTC, datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from pydantic import BaseModel, Field

from . import llm, ratelimit
from .auth import Principal, current_user
from .config import get_settings
from .db import get_db
from .messaging.outbox import enqueue

router = APIRouter(prefix="/ai")

DISCLAIMER = "Sugerencias automatizadas generadas por IA; verifica los detalles del evento antes de ir."
EARTH_RADIUS_M = 6_378_100
CATEGORIES = {
    "cultural": ["cultur", "arte", "teatro", "museo", "cine", "expo", "lectura", "poesía"],
    "musical": ["músic", "music", "concierto", "jazz", "rock", "rumba", "fiesta", "dj", "salsa", "banda", "bailar"],
    "deportivo": ["deport", "fútbol", "futbol", "correr", "partido", "yoga", "bici", "torneo", "ejercicio"],
    "gastronomico": ["comer", "comida", "gastr", "café", "cafe", "cerveza", "almuerzo", "cena", "restaurante", "brunch"],
}


class Location(BaseModel):
    lat: float = Field(ge=-90, le=90)
    lng: float = Field(ge=-180, le=180)


class ChatRequest(BaseModel):
    message: str = Field(min_length=1, max_length=500)
    sessionId: str | None = Field(default=None, pattern=r"^[0-9a-fA-F-]{36}$")
    location: Location | None = None


class RecommendedEvent(BaseModel):
    eventId: str
    title: str
    category: str
    startsAt: str
    venueName: str
    priceCents: int
    isLive: bool


class ChatResponse(BaseModel):
    sessionId: str
    reply: str
    events: list[RecommendedEvent]
    disclaimer: str = DISCLAIMER


def detect_categories(text: str) -> list[str]:
    low = text.lower()
    return [cat for cat, words in CATEGORIES.items() if any(w in low for w in words)]


async def candidate_events(db, *, categories: list[str], location: Location | None, limit: int) -> list[dict]:
    """Eventos publicados que no han terminado, de los próximos 14 días, no agotados."""
    now = datetime.now(UTC)
    base = {"status": "published", "endsAt": {"$gt": now}, "startsAt": {"$lt": now + timedelta(days=14)}, "soldOut": {"$ne": True}}
    if location:
        base["location"] = {"$geoWithin": {"$centerSphere": [[location.lng, location.lat], 15_000 / EARTH_RADIUS_M]}}
    found: list[dict] = []
    if categories:
        found = await db.catalog.find({**base, "category": {"$in": categories}}).sort("startsAt", 1).limit(limit).to_list(limit)
    if len(found) < limit:
        seen = [e["_id"] for e in found]
        found += await db.catalog.find({**base, "_id": {"$nin": seen}}).sort("startsAt", 1).limit(limit - len(found)).to_list(limit)
    return found


def price_label(event: dict) -> str:
    if not event.get("ticketed"):
        return "gratis"
    return f"${event.get('priceCents', 0) // 100:,} COP".replace(",", ".")


def system_prompt(prefs: dict | None, candidates: list[dict], now: datetime) -> str:
    who = []
    if prefs:
        if prefs.get("displayName"):
            who.append(f"Se llama {prefs['displayName']}.")
        if prefs.get("interests"):
            who.append(f"Le interesan planes: {', '.join(prefs['interests'])}.")
        if prefs.get("homeZone"):
            who.append(f"Su zona preferida es {prefs['homeZone']}.")
    lines = [
        f"- id={e['_id']} | {e['title']} | {e['category']} | {e['startsAt'].astimezone(timezone(timedelta(hours=-5))).strftime('%a %d %b %H:%M')}"
        f" | {e['venueName']} | {price_label(e)}"
        f"{' | PARCHE EN VIVO' if e.get('isLive') else ''} | {e.get('description', '')[:160]}"
        for e in candidates
    ]
    return (
        "Eres el asistente de Festa, una app para descubrir planes en la comunidad de la Escuela Colombiana de Ingeniería. "
        "Conversa en español de Colombia, breve y amable (máximo 3 frases), entiende qué le provoca hacer al usuario ahora "
        "y recomienda entre 1 y 3 eventos de la lista CANDIDATOS. Nunca inventes eventos, horarios ni precios; si nada "
        "encaja, dilo y haz una pregunta para afinar. Si el mensaje no tiene que ver con planes o eventos, redirige con "
        "amabilidad. No pidas datos personales.\n"
        f"Fecha y hora actual en Bogotá: {now.astimezone(timezone(timedelta(hours=-5))).strftime('%A %d %B %Y %H:%M')}.\n"
        f"Sobre el usuario: {' '.join(who) or 'sin datos de perfil.'}\n"
        "CANDIDATOS:\n" + ("\n".join(lines) or "(no hay eventos próximos)") + "\n"
        'Responde SOLO con JSON: {"reply": "<tu mensaje>", "eventIds": ["<id de CANDIDATOS>", ...]}'
    )


@router.post("/chat", response_model=ChatResponse)
async def chat(body: ChatRequest, request: Request, response: Response, user: Principal = Depends(current_user)):
    settings = get_settings()
    allowed, retry_after = await ratelimit.hit(user.user_id)
    if not allowed:
        raise HTTPException(status_code=429, detail="vas muy rápido, espera un momento", headers={"Retry-After": str(retry_after)})

    db = get_db()
    session_id = (body.sessionId or str(uuid.uuid4())).lower()
    session = await db.chat_sessions.find_one({"_id": session_id})
    if session and session["userId"] != user.user_id:
        raise HTTPException(status_code=404, detail="conversación no encontrada")
    history = (session or {}).get("messages", [])[-settings.chat_history_turns * 2:]

    prefs = await db.user_prefs.find_one({"_id": user.user_id})
    # RNF5: la ubicación enviada en el chat solo se usa con consentimiento explícito.
    location = body.location if (prefs or {}).get("locationConsent") else None
    categories = detect_categories(body.message) or (prefs or {}).get("interests", [])
    candidates = await candidate_events(db, categories=categories, location=location, limit=settings.chat_max_candidates)

    now = datetime.now(UTC)
    messages = [{"role": m["role"], "content": m["content"]} for m in history] + [{"role": "user", "content": body.message}]
    try:
        result = await llm.complete(system_prompt(prefs, candidates, now), messages, settings.llm_chat_model, json_output=True, max_tokens=500)
        parsed = llm.parse_json(result.text)
    except llm.LLMError as err:
        raise HTTPException(status_code=503, detail="la IA no está disponible ahora, intenta de nuevo") from err

    reply = str(parsed.get("reply") or "").strip()[:1500] or "¿Qué tipo de plan te provoca hoy?"
    by_id = {e["_id"]: e for e in candidates}
    chosen = [i for i in dict.fromkeys(str(x).lower() for x in parsed.get("eventIds") or []) if i in by_id][:5]

    await db.chat_sessions.update_one(
        {"_id": session_id},
        {
            "$setOnInsert": {"userId": user.user_id, "createdAt": now},
            "$set": {"updatedAt": now},
            "$push": {"messages": {"$each": [
                {"role": "user", "content": body.message, "at": now},
                {"role": "assistant", "content": json.dumps({"reply": reply, "eventIds": chosen}, ensure_ascii=False), "at": now},
            ], "$slice": -40}},
        },
        upsert=True,
    )

    await enqueue(
        db,
        exchange="festa.ai",
        routing_key="recommendation.served",
        payload={
            "sessionId": session_id,
            "userId": user.user_id,
            "eventIds": chosen,
            "provider": result.provider[:30],
            "model": result.model[:60],
            "inputTokens": result.input_tokens,
            "outputTokens": result.output_tokens,
            "servedAt": now.isoformat().replace("+00:00", "Z"),
        },
        correlation_id=request.headers.get("x-correlation-id"),
    )

    return ChatResponse(
        sessionId=session_id,
        reply=reply,
        events=[
            RecommendedEvent(
                eventId=e["_id"], title=e["title"], category=e["category"],
                startsAt=e["startsAt"].isoformat().replace("+00:00", "Z"), venueName=e["venueName"],
                priceCents=e.get("priceCents", 0), isLive=bool(e.get("isLive")),
            )
            for e in (by_id[i] for i in chosen)
        ],
    )


@router.get("/chat/sessions/{session_id}")
async def get_session(session_id: str, user: Principal = Depends(current_user)):
    session = await get_db().chat_sessions.find_one({"_id": session_id.lower(), "userId": user.user_id})
    if not session:
        raise HTTPException(status_code=404, detail="conversación no encontrada")
    out = []
    for m in session.get("messages", []):
        content = m["content"]
        if m["role"] == "assistant":
            try:
                content = json.loads(content).get("reply", content)
            except ValueError:
                pass
        out.append({"role": m["role"], "content": content})
    return {"sessionId": session["_id"], "messages": out}


@router.delete("/chat/history", status_code=204)
async def delete_history(user: Principal = Depends(current_user)):
    await get_db().chat_sessions.delete_many({"userId": user.user_id})
    return Response(status_code=204)
