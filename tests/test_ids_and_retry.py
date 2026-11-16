"""Validación de UUID completo y origen de exchange en reintentos (sin Mongo ni RabbitMQ)."""
import re
import uuid
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from app import ids
from app.chat import ChatRequest
from app.descriptions import DescriptionRequest
from app.messaging.consumer import _origin_exchange


def test_uuid_pattern_acepta_uuid_y_rechaza_36_guiones():
    assert re.match(ids.UUID_PATTERN, str(uuid.uuid4()))
    assert not re.match(ids.UUID_PATTERN, "-" * 36)
    assert not re.match(ids.UUID_PATTERN, "0" * 8 + "-" * 28)


def test_chat_rechaza_session_id_que_no_es_uuid():
    with pytest.raises(ValidationError):
        ChatRequest(message="hola", sessionId="-" * 36)
    assert ChatRequest(message="hola", sessionId=str(uuid.uuid4())).sessionId


def test_descripcion_rechaza_draft_id_que_no_es_uuid():
    base = {"name": "Feria", "location": "Bloque G", "activityType": "stand"}
    with pytest.raises(ValidationError):
        DescriptionRequest(draftId="-" * 36, **base)
    assert DescriptionRequest(draftId=str(uuid.uuid4()), **base).draftId


def test_canonical_normaliza_a_minusculas():
    u = str(uuid.uuid4())
    assert ids.canonical(u.upper()) == u


def test_reintento_toma_el_exchange_de_origen_desde_x_death():
    msg = SimpleNamespace(exchange="", headers={"x-death": [{"exchange": "festa.ai", "reason": "expired"}]})
    assert _origin_exchange(msg) == "festa.ai"


def test_entrega_inicial_usa_su_exchange():
    msg = SimpleNamespace(exchange="festa.event", headers={})
    assert _origin_exchange(msg) == "festa.event"


def test_mensaje_sin_exchange_ni_x_death_queda_vacio():
    msg = SimpleNamespace(exchange="", headers=None)
    assert _origin_exchange(msg) == ""
