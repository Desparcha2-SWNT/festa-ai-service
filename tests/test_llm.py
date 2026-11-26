import pytest

from app import llm


async def test_fake_description_uses_given_fields():
    result = await llm.complete("redactor", [{"role": "user", "content": "- nombre: Noche de jazz\n- lugar: Bloque G\n- actividad: concierto"}], "m")
    assert "Noche de jazz" in result.text and "Bloque G" in result.text
    assert result.provider == "fake" and result.input_tokens > 0


async def test_fake_chat_only_picks_candidate_ids():
    system = 'CANDIDATOS:\n- {"id": "6f1c2b8e-3a1d-4c3e-9b0a-1f2e3d4c5b6a", "titulo": "Jazz"}'
    result = await llm.complete(system, [{"role": "user", "content": "quiero música"}], "m", json_output=True)
    assert llm.parse_json(result.text)["eventIds"] == ["6f1c2b8e-3a1d-4c3e-9b0a-1f2e3d4c5b6a"]


def test_parse_json_strips_code_fences():
    assert llm.parse_json('```json\n{"reply": "hola", "eventIds": []}\n```') == {"reply": "hola", "eventIds": []}


def test_parse_json_rejects_garbage():
    with pytest.raises(llm.LLMError):
        llm.parse_json("no hay json aquí")


@pytest.mark.parametrize(("key", "expected"), [("none", "fake"), ("", "fake"), (" NONE ", "fake"), ("gsk-real", "groq")])
def test_api_key_none_means_local_provider(key, expected):
    from app.config import Settings

    assert Settings(llm_provider="groq", llm_api_key=key).effective_provider == expected


def _mock_provider(monkeypatch, handler, provider="groq"):
    """Enruta las llamadas HTTP del cliente a `handler`, con `provider` y una llave configurados."""
    import httpx

    from app import config

    settings = config.Settings(llm_provider=provider, llm_api_key="gsk-test")
    monkeypatch.setattr(llm, "get_settings", lambda: settings)
    real = httpx.AsyncClient
    monkeypatch.setattr(llm.httpx, "AsyncClient", lambda **kw: real(transport=httpx.MockTransport(handler), **kw))


async def test_groq_request_and_usage(monkeypatch):
    import json

    import httpx

    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        seen["auth"] = request.headers["authorization"]
        seen["body"] = json.loads(request.content)
        return httpx.Response(
            200,
            json={
                "choices": [{"message": {"content": '{"reply": "hola", "eventIds": []}'}}],
                "usage": {"prompt_tokens": 12, "completion_tokens": 7},
            },
        )

    _mock_provider(monkeypatch, handler)
    result = await llm.complete("Responde SOLO con JSON", [{"role": "user", "content": "hola"}], "llama-3.3-70b-versatile", json_output=True, max_tokens=500)

    assert seen["url"] == "https://api.groq.com/openai/v1/chat/completions"
    assert seen["auth"] == "Bearer gsk-test"
    assert seen["body"]["model"] == "llama-3.3-70b-versatile"
    assert seen["body"]["messages"][0] == {"role": "system", "content": "Responde SOLO con JSON"}
    assert seen["body"]["response_format"] == {"type": "json_object"}
    assert seen["body"]["max_completion_tokens"] == 500
    assert (result.provider, result.input_tokens, result.output_tokens) == ("groq", 12, 7)
    assert llm.parse_json(result.text) == {"reply": "hola", "eventIds": []}


async def test_groq_error_status_is_llm_error(monkeypatch):
    import httpx

    _mock_provider(monkeypatch, lambda request: httpx.Response(429, json={"error": {"message": "rate limit"}}))
    with pytest.raises(llm.LLMError, match="groq 429"):
        await llm.complete("s", [{"role": "user", "content": "hola"}], "m")


async def test_groq_response_without_choices_is_llm_error(monkeypatch):
    import httpx

    _mock_provider(monkeypatch, lambda request: httpx.Response(200, json={"choices": []}))
    with pytest.raises(llm.LLMError, match="groq sin texto"):
        await llm.complete("s", [{"role": "user", "content": "hola"}], "m")


def test_gemini_is_no_longer_a_provider():
    from app.config import Settings

    assert Settings(llm_provider="groq", llm_api_key="gsk").effective_provider == "groq"
    assert "gemini" not in llm._OPENAI_COMPATIBLE
