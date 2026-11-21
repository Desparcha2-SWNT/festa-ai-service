"""Cliente de IA Generativa con varios proveedores (LLM_PROVIDER).

Todos devuelven un `LLMResult` con el texto y los tokens usados (para el costo de IA en analytics).
`fake` es un generador local sin costo: se usa cuando no hay LLM_API_KEY, para desarrollo y pruebas.
"""
from __future__ import annotations

import asyncio
import json
import re
from dataclasses import dataclass

import httpx

from .config import get_settings


class LLMError(Exception):
    """Falla del proveedor (red, cuota, respuesta inválida). La API responde 503."""


@dataclass
class LLMResult:
    text: str
    provider: str
    model: str
    input_tokens: int
    output_tokens: int


Message = dict  # {"role": "user" | "assistant", "content": str}


_slots: asyncio.Semaphore | None = None


def _semaphore() -> asyncio.Semaphore:
    # Tope de llamadas simultáneas al proveedor en este proceso: limita costo y cuota.
    global _slots
    if _slots is None:
        _slots = asyncio.Semaphore(get_settings().llm_max_concurrency)
    return _slots


async def complete(system: str, messages: list[Message], model: str, *, json_output: bool = False, max_tokens: int = 1024) -> LLMResult:
    async with _semaphore():
        return await _complete(system, messages, model, json_output=json_output, max_tokens=max_tokens)


async def _complete(system: str, messages: list[Message], model: str, *, json_output: bool, max_tokens: int) -> LLMResult:
    settings = get_settings()
    provider = settings.effective_provider
    try:
        if provider == "fake":
            return _fake(system, messages, model, json_output)
        async with httpx.AsyncClient(timeout=settings.llm_timeout_s) as http:
            if provider == "gemini":
                return await _gemini(http, settings.llm_api_key, system, messages, model, json_output, max_tokens)
            if provider == "openai":
                return await _openai(http, settings.llm_api_key, system, messages, model, json_output, max_tokens)
            if provider == "ollama":
                return await _ollama(http, settings.ollama_url, system, messages, model, json_output, max_tokens)
        if provider == "anthropic":
            return await _anthropic(settings.llm_api_key, system, messages, model, max_tokens)
    except LLMError:
        raise
    except Exception as err:  # noqa: BLE001 - cualquier falla del proveedor se reporta igual
        raise LLMError(f"{provider}: {err}") from err
    raise LLMError(f"LLM_PROVIDER desconocido: {provider}")


async def _gemini(http, key, system, messages, model, json_output, max_tokens) -> LLMResult:
    body = {
        "systemInstruction": {"parts": [{"text": system}]},
        "contents": [
            {"role": "model" if m["role"] == "assistant" else "user", "parts": [{"text": m["content"]}]} for m in messages
        ],
        "generationConfig": {"maxOutputTokens": max_tokens, **({"responseMimeType": "application/json"} if json_output else {})},
    }
    res = await http.post(
        f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent",
        headers={"x-goog-api-key": key},
        json=body,
    )
    if res.status_code != 200:
        raise LLMError(f"gemini {res.status_code}: {res.text[:300]}")
    data = res.json()
    try:
        text = "".join(p.get("text", "") for p in data["candidates"][0]["content"]["parts"])
    except (KeyError, IndexError) as err:
        raise LLMError(f"gemini sin texto: {str(data)[:300]}") from err
    usage = data.get("usageMetadata", {})
    return LLMResult(text, "gemini", model, usage.get("promptTokenCount", 0), usage.get("candidatesTokenCount", 0))


async def _openai(http, key, system, messages, model, json_output, max_tokens) -> LLMResult:
    body = {
        "model": model,
        "messages": [{"role": "system", "content": system}, *messages],
        "max_completion_tokens": max_tokens,
        **({"response_format": {"type": "json_object"}} if json_output else {}),
    }
    res = await http.post("https://api.openai.com/v1/chat/completions", headers={"Authorization": f"Bearer {key}"}, json=body)
    if res.status_code != 200:
        raise LLMError(f"openai {res.status_code}: {res.text[:300]}")
    data = res.json()
    usage = data.get("usage", {})
    return LLMResult(data["choices"][0]["message"]["content"] or "", "openai", model, usage.get("prompt_tokens", 0), usage.get("completion_tokens", 0))


async def _ollama(http, base_url, system, messages, model, json_output, max_tokens) -> LLMResult:
    # num_predict es el tope de tokens de salida en Ollama (equivale a maxOutputTokens de Gemini).
    body = {
        "model": model,
        "stream": False,
        "messages": [{"role": "system", "content": system}, *messages],
        "options": {"num_predict": max_tokens},
        **({"format": "json"} if json_output else {}),
    }
    res = await http.post(f"{base_url.rstrip('/')}/api/chat", json=body)
    if res.status_code != 200:
        raise LLMError(f"ollama {res.status_code}: {res.text[:300]}")
    data = res.json()
    return LLMResult(data["message"]["content"], "ollama", model, data.get("prompt_eval_count", 0), data.get("eval_count", 0))


async def _anthropic(key, system, messages, model, max_tokens) -> LLMResult:
    # SDK oficial. Para RNF8 se recomienda claude-haiku-5-5 con esfuerzo bajo (tareas cortas).
    import anthropic

    client = anthropic.AsyncAnthropic(api_key=key, timeout=get_settings().llm_timeout_s, max_retries=1)
    try:
        response = await client.messages.create(
            model=model,
            # El tope lo pone quien llama (chat 500, descripciones 400): es lo que se factura.
            max_tokens=max_tokens,
            system=system,
            messages=messages,
            output_config={"effort": "low"},
        )
    except anthropic.RateLimitError as err:
        raise LLMError("anthropic: límite de tasa") from err
    except anthropic.APIStatusError as err:
        raise LLMError(f"anthropic {err.status_code}: {err.message}") from err
    except anthropic.APIConnectionError as err:
        raise LLMError("anthropic: sin conexión") from err
    finally:
        await client.close()
    if response.stop_reason == "refusal":
        raise LLMError("anthropic: el modelo declinó la solicitud")
    text = "".join(block.text for block in response.content if block.type == "text")
    return LLMResult(text, "anthropic", model, response.usage.input_tokens, response.usage.output_tokens)


# ----------------------------------------------------------------- proveedor local sin costo

def _fake(system: str, messages: list[Message], model: str, json_output: bool) -> LLMResult:
    """Respuestas deterministas y razonables sin llamar a nadie (desarrollo, pruebas, demo sin llave)."""
    last = messages[-1]["content"] if messages else ""
    # Los candidatos viajan en un mensaje de datos (ver chat.context_message), no en el system prompt.
    context = system + "\n" + "\n".join(m["content"] for m in messages)
    if "CANDIDATOS" in context:
        # Los candidatos van como JSON por línea: "id": "<uuid>".
        ids = re.findall(r'"id": "([0-9a-f-]{36})"', context)[:3]
        reply = (
            "¡Tengo algunos planes que te pueden gustar! Te dejo los que mejor encajan con lo que me contaste."
            if ids
            else "Por ahora no veo eventos que encajen. ¿Quieres que busque otra categoría o para otro día?"
        )
        text = json.dumps({"reply": reply, "eventIds": ids}, ensure_ascii=False)
    else:
        fields = dict(re.findall(r"^- (\w+): (.+)$", last, flags=re.M))
        name = fields.get("nombre", "este plan")
        place = fields.get("lugar", "un lugar especial")
        activity = fields.get("actividad", "una experiencia distinta")
        text = (
            f"Vive {name} en {place}: {activity} pensado para compartir con amigos y descubrir algo nuevo. "
            "Llega con tiempo, trae buena energía y prepárate para una experiencia que vas a querer repetir."
        )
        if json_output:
            text = json.dumps({"description": text}, ensure_ascii=False)
    tokens_in = max(1, (len(system) + sum(len(m["content"]) for m in messages)) // 4)
    return LLMResult(text, "fake", model, tokens_in, max(1, len(text) // 4))


def parse_json(text: str) -> dict:
    """Extrae el objeto JSON de la respuesta (algunos modelos lo envuelven en ```json ... ```)."""
    match = re.search(r"\{.*\}", text, flags=re.S)
    if not match:
        raise LLMError("la IA no devolvió JSON")
    try:
        return json.loads(match.group(0))
    except json.JSONDecodeError as err:
        raise LLMError("la IA devolvió JSON inválido") from err
