"""Supresión de cuentas frente a peticiones y entregas en curso (Ley 1581), contra MongoDB real.

Las carreras se reproducen de forma determinista: el borrado se ejecuta dentro del LLM falso o justo
antes de la escritura del handler, que es el intercalado que una prueba secuencial no ejercita.
"""
import sys
import types
import uuid

import pytest

from app import llm, ratelimit
from app.messaging import handlers
from app.messaging.handlers import HANDLERS

from .conftest import HAS_CONTRACTS, contract_example, verified_merchant
from .test_api import USER, client

pytestmark = pytest.mark.skipif(not HAS_CONTRACTS, reason="sin festa-infra/contracts")

DELETED = {"userId": USER, "deletedAt": "2026-11-17T12:00:00Z"}


@pytest.fixture(autouse=True)
def _fresh_limiter(monkeypatch):
    monkeypatch.setattr(ratelimit, "_local", {})


def _llm_that_runs(monkeypatch, side_effect):
    """El LLM falso ejecuta `side_effect` mientras «piensa»: el borrado ocurre con la petición en curso."""
    original = llm.complete

    async def complete(*args, **kwargs):
        await side_effect()
        return await original(*args, **kwargs)

    monkeypatch.setattr(llm, "complete", complete)


def _description_body(**extra) -> dict:
    return {"draftId": str(uuid.uuid4()), "name": "Noche de jazz", "location": "Bloque G", "activityType": "concierto", **extra}


async def _user_rows_in_outbox(db) -> int:
    return await db.outbox.count_documents({"$or": [{"payload.userId": USER}, {"payload.requestedBy": USER}]})


async def test_token_vigente_de_cuenta_borrada_no_puede_chatear_ni_generar(mongo_db):
    await HANDLERS["account.deleted"](DELETED, {}, mongo_db)
    async with client() as c:
        chat = await c.post("/ai/chat", json={"message": "hola"})
        desc = await c.post("/ai/descriptions", json=_description_body())
    assert chat.status_code == 403 and desc.status_code == 403
    assert await mongo_db.chat_sessions.count_documents({}) == 0
    assert await mongo_db.descriptions.count_documents({}) == 0


async def test_chat_en_curso_cuando_se_borra_la_cuenta_no_deja_historial_ni_mensaje(mongo_db, monkeypatch):
    _llm_that_runs(monkeypatch, lambda: HANDLERS["account.deleted"](DELETED, {}, mongo_db))
    async with client() as c:
        res = await c.post("/ai/chat", json={"message": "quiero música"})
    assert res.status_code == 403
    assert await mongo_db.chat_sessions.count_documents({"userId": USER}) == 0
    assert await _user_rows_in_outbox(mongo_db) == 0


async def test_descripcion_en_curso_cuando_se_borra_la_cuenta_queda_anonima_y_sin_mensaje(mongo_db, monkeypatch):
    _llm_that_runs(monkeypatch, lambda: HANDLERS["account.deleted"](DELETED, {}, mongo_db))
    body = _description_body(merchantId=await verified_merchant(mongo_db, USER))
    async with client() as c:
        res = await c.post("/ai/descriptions", json=body)
    assert res.status_code == 403
    saved = await mongo_db.descriptions.find_one({"draftId": body["draftId"]})
    assert saved["requestedBy"] is None and "pendingEvents" not in saved
    assert await _user_rows_in_outbox(mongo_db) == 0


async def test_account_deleted_cancela_mensajes_pendientes_y_sin_publicar(mongo_db):
    async with client() as c:
        assert (await c.post("/ai/chat", json={"message": "hola"})).status_code == 200
        merchant = await verified_merchant(mongo_db, USER)
        assert (await c.post("/ai/descriptions", json=_description_body(merchantId=merchant))).status_code == 200
    # Uno ya publicado (lo atiende cada consumidor con su account.deleted) y uno pendiente dentro del documento.
    await mongo_db.outbox.update_one({"routingKey": "recommendation.served"}, {"$set": {"sentAt": "ya"}})
    pending = {"eventId": str(uuid.uuid4()), "payload": {"requestedBy": USER}}
    await mongo_db.descriptions.update_many({}, {"$push": {"pendingEvents": pending}})

    await HANDLERS["account.deleted"](DELETED, {}, mongo_db)

    unsent = await mongo_db.outbox.count_documents({"sentAt": None, "$or": [{"payload.userId": USER}, {"payload.requestedBy": USER}]})
    assert unsent == 0
    assert await mongo_db.outbox.count_documents({"sentAt": "ya"}) == 1
    # La copia ya publicada se conserva, pero sin el id de la persona.
    sent = await mongo_db.outbox.find_one({"sentAt": "ya"})
    assert sent["payload"]["userId"] == "00000000-0000-0000-0000-000000000000"
    assert await mongo_db.outbox.count_documents({"$or": [{"payload.userId": USER}, {"payload.requestedBy": USER}]}) == 0
    assert await mongo_db.descriptions.count_documents({"pendingEvents": {"$exists": True}}) == 0


async def test_borrar_historial_con_un_turno_en_curso_no_lo_deja_reaparecer(mongo_db, monkeypatch):
    async with client() as c:
        first = await c.post("/ai/chat", json={"message": "hola"})
        session_id = first.json()["sessionId"]

        async def clear_history():
            assert (await c.delete("/ai/chat/history")).status_code == 204

        _llm_that_runs(monkeypatch, clear_history)
        res = await c.post("/ai/chat", json={"message": "y algo de jazz?", "sessionId": session_id})
    assert res.status_code == 200  # la respuesta se entrega, pero no se guarda
    assert await mongo_db.chat_sessions.count_documents({"userId": USER}) == 0
    # Borrar el historial no es borrar la cuenta: el conteo de tokens se publica igual.
    assert await mongo_db.outbox.count_documents({"routingKey": "recommendation.served"}) == 2


async def test_borrar_historial_antes_de_un_turno_nuevo_no_lo_afecta(mongo_db):
    async with client() as c:
        await c.post("/ai/chat", json={"message": "hola"})
        assert (await c.delete("/ai/chat/history")).status_code == 204
        res = await c.post("/ai/chat", json={"message": "de nuevo"})
    assert res.status_code == 200
    assert await mongo_db.chat_sessions.count_documents({"userId": USER}) == 1


def _delete_during_write(monkeypatch, db):
    """account.deleted corre después de que el handler consultó la lápida y antes de su escritura."""
    original = handlers._upsert_if_allowed

    async def racing(*args, **kwargs):
        await HANDLERS["account.deleted"](DELETED, {}, db)
        await original(*args, **kwargs)

    monkeypatch.setattr(handlers, "_upsert_if_allowed", racing)


async def test_perfil_en_carrera_con_el_borrado_no_recrea_preferencias(mongo_db, monkeypatch):
    _delete_during_write(monkeypatch, mongo_db)
    await HANDLERS["profile.updated"](
        {"userId": USER, "displayName": "Ana", "interests": ["musical"], "homeZone": "Usaquén", "updatedAt": "2026-11-17T11:00:00Z"},
        {}, mongo_db,
    )
    assert await mongo_db.user_prefs.count_documents({"_id": USER}) == 0


async def test_consentimiento_en_carrera_con_el_borrado_no_recrea_preferencias(mongo_db, monkeypatch):
    _delete_during_write(monkeypatch, mongo_db)
    await HANDLERS["location_consent.granted"]({"userId": USER, "grantedAt": "2026-11-17T11:00:00Z"}, {}, mongo_db)
    assert await mongo_db.user_prefs.count_documents({"_id": USER}) == 0


def _merchant(**extra) -> dict:
    data = contract_example("user", "merchant.updated")
    data.update(userId=USER, **extra)
    return data


async def test_borrado_desvincula_el_comercio_y_una_entrega_tardia_no_lo_restablece(mongo_db):
    data = _merchant(updatedAt="2026-11-17T10:00:00Z")
    await HANDLERS["merchant.updated"](data, {}, mongo_db)
    await HANDLERS["account.deleted"](DELETED, {}, mongo_db)
    await HANDLERS["merchant.updated"](_merchant(updatedAt="2026-11-17T13:00:00Z"), {}, mongo_db)
    stored = await mongo_db.merchants.find_one({"_id": data["merchantId"]})
    assert stored["userId"] is None and stored["verified"] is False


async def test_comercio_en_carrera_con_el_borrado_queda_desvinculado(mongo_db, monkeypatch):
    _delete_during_write(monkeypatch, mongo_db)
    data = _merchant(updatedAt="2026-11-17T10:00:00Z")
    await HANDLERS["merchant.updated"](data, {}, mongo_db)
    stored = await mongo_db.merchants.find_one({"_id": data["merchantId"]})
    assert stored["userId"] is None and stored["verified"] is False


async def test_descripcion_exige_comercio_verificado_y_respeta_la_revocacion(mongo_db):
    data = _merchant(verified=True, updatedAt="2026-11-17T10:00:00Z")
    await HANDLERS["merchant.updated"](data, {}, mongo_db)
    async with client() as c:
        ok = await c.post("/ai/descriptions", json=_description_body(merchantId=data["merchantId"]))
        await HANDLERS["merchant.updated"](_merchant(verified=False, updatedAt="2026-11-17T11:00:00Z"), {}, mongo_db)
        revoked = await c.post("/ai/descriptions", json=_description_body(merchantId=data["merchantId"]))
    assert ok.status_code == 200
    assert revoked.status_code == 403


def _verified(**extra) -> dict:
    data = contract_example("user", "merchant.verified")
    data.update(userId=USER, **extra)
    return data


async def test_comercio_recien_aprobado_puede_pedir_descripciones(mongo_db):
    # Al aprobar, user-service solo publica merchant.verified: antes ai respondía 403 hasta que el comercio editara su perfil.
    data = _verified(verifiedAt="2026-11-17T10:00:00Z")
    await HANDLERS["merchant.verified"](data, {}, mongo_db)
    async with client() as c:
        res = await c.post("/ai/descriptions", json=_description_body(merchantId=data["merchantId"]))
    stored = await mongo_db.merchants.find_one({"_id": data["merchantId"]})
    assert res.status_code == 200
    assert stored["verified"] is True and stored["userId"] == USER and stored["businessName"] == data["businessName"]


async def test_verificacion_atrasada_no_pisa_una_revocacion_posterior(mongo_db):
    data = _verified(verifiedAt="2026-11-17T10:00:00Z")
    await HANDLERS["merchant.updated"](_merchant(merchantId=data["merchantId"], verified=False, updatedAt="2026-11-17T11:00:00Z"), {}, mongo_db)
    await HANDLERS["merchant.verified"](data, {}, mongo_db)
    stored = await mongo_db.merchants.find_one({"_id": data["merchantId"]})
    assert stored["verified"] is False


async def test_verificacion_tras_el_borrado_no_revincula_el_comercio(mongo_db):
    await HANDLERS["account.deleted"](DELETED, {}, mongo_db)
    data = _verified(verifiedAt="2026-11-17T13:00:00Z")
    await HANDLERS["merchant.verified"](data, {}, mongo_db)
    stored = await mongo_db.merchants.find_one({"_id": data["merchantId"]})
    assert stored is None or (stored["userId"] is None and stored["verified"] is False)


async def test_anthropic_respeta_el_tope_de_tokens_de_quien_llama(monkeypatch):
    seen = {}

    class Messages:
        async def create(self, **kwargs):
            seen.update(kwargs)
            block = types.SimpleNamespace(type="text", text="ok")
            usage = types.SimpleNamespace(input_tokens=1, output_tokens=1)
            return types.SimpleNamespace(content=[block], usage=usage, stop_reason="end_turn")

    class Client:
        def __init__(self, **_kwargs):
            self.messages = Messages()

        async def close(self):
            pass

    fake = types.SimpleNamespace(
        AsyncAnthropic=Client, RateLimitError=type("E1", (Exception,), {}),
        APIStatusError=type("E2", (Exception,), {}), APIConnectionError=type("E3", (Exception,), {}),
    )
    monkeypatch.setitem(sys.modules, "anthropic", fake)
    await llm._anthropic("k", "sistema", [{"role": "user", "content": "hola"}], "claude-haiku-5-5", 400)
    assert seen["max_tokens"] == 400


async def _doc_with_pending(db, user_id):
    from app.messaging.pending import pending_event

    ev = pending_event(
        exchange="festa.ai",
        routing_key="description.generated",
        payload={"requestedBy": user_id, "eventId": str(uuid.uuid4())},
    )
    await db.descriptions.insert_one({"_id": "d1", "pendingEvents": [ev]})
    return ev


async def test_relay_no_publica_pendientes_de_una_cuenta_ya_borrada(mongo_db):
    from app.messaging.pending import relay

    ev = await _doc_with_pending(mongo_db, USER)
    await mongo_db.deleted_accounts.insert_one({"_id": USER})

    await relay(mongo_db, "descriptions", "d1", [ev])  # el barrido ya tenía el pendiente en memoria

    assert await mongo_db.outbox.count_documents({}) == 0
    assert not (await mongo_db.descriptions.find_one({"_id": "d1"})).get("pendingEvents")


async def test_relay_cancela_lo_que_encolo_si_la_cuenta_se_borra_en_medio(mongo_db, monkeypatch):
    from app.messaging import pending

    ev = await _doc_with_pending(mongo_db, USER)
    original = pending.enqueue

    async def enqueue_then_delete(db, **kwargs):
        result = await original(db, **kwargs)
        await db.deleted_accounts.insert_one({"_id": USER})  # account.deleted corre justo después de encolar
        return result

    monkeypatch.setattr(pending, "enqueue", enqueue_then_delete)
    await pending.relay(mongo_db, "descriptions", "d1", [ev])

    assert await mongo_db.outbox.count_documents({}) == 0


async def test_relay_sigue_publicando_las_cuentas_vigentes(mongo_db):
    from app.messaging.pending import relay

    ev = await _doc_with_pending(mongo_db, USER)

    await relay(mongo_db, "descriptions", "d1", [ev])

    assert await mongo_db.outbox.count_documents({"_id": ev["eventId"]}) == 1


async def test_las_filas_enviadas_del_outbox_expiran(mongo_db):
    indexes = await mongo_db.outbox.index_information()
    ttl = indexes["outbox_sent_ttl"]
    assert ttl["key"] == [("sentAt", 1)] and ttl["expireAfterSeconds"] == 30 * 24 * 3600


class _FakeExchange:
    def __init__(self):
        self.published = []

    async def publish(self, message, routing_key):
        self.published.append((routing_key, message.body))


class _ChannelThatPausesAfterClaim:
    """Ejecuta `side_effect` cuando el publicador ya reclamó la fila y antes de que publique."""

    def __init__(self, side_effect):
        self.exchange = _FakeExchange()
        self._side_effect = side_effect

    async def get_exchange(self, name, ensure=False):
        await self._side_effect()
        return self.exchange


async def _outbox_row_for(db, user_id):
    from app.messaging.outbox import enqueue

    return await enqueue(
        db, exchange="festa.ai", routing_key="recommendation.served", payload={"userId": user_id, "eventIds": []}
    )


async def test_publicador_no_envia_lo_reclamado_si_la_cuenta_se_borra_antes_de_publicar(mongo_db):
    from app.messaging.publisher import publish_pending

    await _outbox_row_for(mongo_db, USER)

    async def delete_account():
        await HANDLERS["account.deleted"](DELETED, {}, mongo_db)

    channel = _ChannelThatPausesAfterClaim(delete_account)
    sent = await publish_pending(channel, mongo_db)

    assert sent == 0
    assert channel.exchange.published == []
    assert await mongo_db.outbox.count_documents({"payload.userId": USER}) == 0


async def test_publicador_publica_normalmente_si_la_cuenta_sigue_vigente(mongo_db):
    from app.messaging.publisher import publish_pending

    await _outbox_row_for(mongo_db, USER)

    async def nothing():
        return None

    channel = _ChannelThatPausesAfterClaim(nothing)
    assert await publish_pending(channel, mongo_db) == 1
    assert len(channel.exchange.published) == 1
