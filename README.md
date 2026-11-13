# festa-ai-service

RF8 y RF9: recomendaciones conversacionales de eventos y descripciones redactadas con IA Generativa.
Python 3.12 / FastAPI con MongoDB (`ai_db`) y Redis (límite por usuario). Puerto 8086.

## Qué hace

**RF9 — Descripciones (`POST /ai/descriptions`).** A partir de nombre, lugar y tipo de actividad (más categoría,
fecha y notas opcionales) redacta una descripción atractiva sin inventar datos. **RNF8:** se genera una sola vez por
borrador (`draftId`) y se guarda; pedirla otra vez devuelve la guardada (`cached: true`) sin costo. Publica
`description.generated` con los tokens usados. Si se envía `merchantId`, debe ser un comercio verificado del usuario.

**RF8 — Chat de recomendaciones (`POST /ai/chat`).** Conversa sobre lo que al usuario le provoca hacer y recomienda
eventos **reales** del catálogo local (alimentado por `event.*`): publicados, sin terminar, de los próximos 14 días y no
agotados, priorizando la categoría detectada en el mensaje o los intereses del perfil. El modelo solo puede elegir ids de
esa lista (se validan). La ubicación que envía la app solo se usa si el usuario dio consentimiento (RNF5). **RNF8:**
modelo de bajo costo y `AI_CHAT_RATE_LIMIT_PER_MIN` mensajes por usuario por minuto (429 con `Retry-After`). Cada
respuesta incluye un `disclaimer` (RN6: sugerencias automatizadas de bajo impacto). Publica `recommendation.served`.

| Método y ruta | Qué hace |
|---|---|
| `POST /ai/descriptions` `{draftId, name, location, activityType, category?, startsAt?, notes?, merchantId?}` | RF9 |
| `POST /ai/chat` `{message, sessionId?, location?: {lat, lng}}` | RF8: `{sessionId, reply, events[], disclaimer}` |
| `GET /ai/chat/sessions/{id}` | Historial de una conversación propia |
| `DELETE /ai/chat/history` | Borra el historial del usuario |
| `GET /health`, `GET /metrics` | Estado (incluye el proveedor activo) y métricas |

El cliente las llama con el prefijo `/api` (Kong lo quita y valida el JWT).

## Proveedores de IA

`LLM_PROVIDER` = `gemini` (por defecto), `openai`, `anthropic` u `ollama`; el modelo se elige con `LLM_CHAT_MODEL` y
`LLM_DESCRIPTION_MODEL`. Sugeridos por costo (RNF8): `gemini-2.5-flash`, `gpt-4o-mini`, `claude-haiku-5-5`.
Sin `LLM_API_KEY` el servicio usa el proveedor local **`fake`**: respuestas de ejemplo sin costo, para desarrollo,
pruebas y demos.

## Mensajería

- **Consume** (`ai-service.inbox`): `event.published|updated|cancelled|expired|sold_out` (catálogo), `profile.updated`
  (intereses), `location_consent.*`, `merchant.updated` (contexto del comercio) y `account.deleted` (borra historial y
  preferencias; Ley 1581). El historial de chat además expira a los 30 días.
- **Publica** (`festa.ai`): `description.generated`, `recommendation.served` (costo de IA y visibilidad para analytics).

## Correr en local

```bash
python -m venv .venv && . .venv/bin/activate      # en Windows: .venv\Scripts\activate
pip install -r requirements-dev.txt
TEST_MONGO_URL=mongodb://localhost:27018/ai_test python -m pytest
uvicorn app.main:app --port 8086 --reload
```

Con el resto del stack: `docker compose --profile ai up -d --build` desde `festa-infra`.
