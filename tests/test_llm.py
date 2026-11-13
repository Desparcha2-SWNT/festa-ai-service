import pytest

from app import llm


async def test_fake_description_uses_given_fields():
    result = await llm.complete("redactor", [{"role": "user", "content": "- nombre: Noche de jazz\n- lugar: Bloque G\n- actividad: concierto"}], "m")
    assert "Noche de jazz" in result.text and "Bloque G" in result.text
    assert result.provider == "fake" and result.input_tokens > 0


async def test_fake_chat_only_picks_candidate_ids():
    system = "CANDIDATOS:\n- id=6f1c2b8e-3a1d-4c3e-9b0a-1f2e3d4c5b6a | Jazz"
    result = await llm.complete(system, [{"role": "user", "content": "quiero música"}], "m", json_output=True)
    assert llm.parse_json(result.text)["eventIds"] == ["6f1c2b8e-3a1d-4c3e-9b0a-1f2e3d4c5b6a"]


def test_parse_json_strips_code_fences():
    assert llm.parse_json('```json\n{"reply": "hola", "eventIds": []}\n```') == {"reply": "hola", "eventIds": []}


def test_parse_json_rejects_garbage():
    with pytest.raises(llm.LLMError):
        llm.parse_json("no hay json aquí")


@pytest.mark.parametrize(("key", "expected"), [("none", "fake"), ("", "fake"), (" NONE ", "fake"), ("AIza-real", "gemini")])
def test_api_key_none_means_local_provider(key, expected):
    from app.config import Settings

    assert Settings(llm_provider="gemini", llm_api_key=key).effective_provider == expected
