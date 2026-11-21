"""Validación de UUID completo y origen de exchange en reintentos (sin Mongo ni RabbitMQ)."""
import re
import uuid
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from app import ids
from app.chat import ChatRequest
from app.descriptions import DescriptionRequest
from app.messaging.consumer import ORIGIN_HEADER, SIGNATURE_HEADER, _origin_exchange, _sign_origin


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


def _signed(origin, body=b"{}"):
    return SimpleNamespace(
        exchange="",
        body=body,
        headers={ORIGIN_HEADER: origin, SIGNATURE_HEADER: _sign_origin(origin, body), "x-retry-count": 1},
    )


def test_reintento_toma_el_exchange_de_origen_de_la_cabecera_firmada():
    # Al volver por la cola de reintento el exchange es "": el origen viaja en ORIGIN_HEADER, firmado por este servicio.
    assert _origin_exchange(_signed("festa.ai")) == "festa.ai"


def test_reintento_sin_firma_no_se_acepta():
    # Quien publica directo en la inbox puede poner la cabecera, pero no puede firmarla.
    msg = SimpleNamespace(exchange="", body=b"{}", headers={ORIGIN_HEADER: "festa.ai", "x-retry-count": 1})
    assert _origin_exchange(msg) == ""


def test_reintento_con_firma_falsa_o_reutilizada_no_se_acepta():
    forged = SimpleNamespace(exchange="", body=b"{}", headers={ORIGIN_HEADER: "festa.ai", SIGNATURE_HEADER: "00" * 32})
    assert _origin_exchange(forged) == ""
    # La firma de otro contenido no se puede reutilizar en este mensaje.
    other_body = _signed("festa.ai", body=b'{"otro":1}')
    assert _origin_exchange(SimpleNamespace(exchange="", body=b"{}", headers=other_body.headers)) == ""
    # Ni se puede cambiar el exchange conservando la firma de otro.
    other_origin = _signed("festa.user")
    swapped = SimpleNamespace(exchange="", body=b"{}", headers={**other_origin.headers, ORIGIN_HEADER: "festa.ai"})
    assert _origin_exchange(swapped) == ""


def test_reintento_sin_cabecera_de_origen_no_se_acepta():
    msg = SimpleNamespace(exchange="", body=b"{}", headers={"x-death": [{"exchange": "festa.ai"}]})
    assert _origin_exchange(msg) == ""


def test_entrega_inicial_usa_su_exchange():
    msg = SimpleNamespace(exchange="festa.event", headers={})
    assert _origin_exchange(msg) == "festa.event"


def test_mensaje_sin_exchange_ni_x_death_queda_vacio():
    msg = SimpleNamespace(exchange="", headers=None)
    assert _origin_exchange(msg) == ""


def test_id_determinista_es_uuid_v8_estable():
    import uuid as _uuid

    from app.messaging.outbox import deterministic_uuid

    first = deterministic_uuid("description.generated:abc")
    assert first == deterministic_uuid("description.generated:abc")
    assert first != deterministic_uuid("description.generated:abd")
    assert _uuid.UUID(first).version == 8
