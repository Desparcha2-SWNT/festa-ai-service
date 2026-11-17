import uuid

import pytest

from app import ratelimit


@pytest.mark.asyncio
async def test_tope_diario_por_usuario_permite_hasta_el_limite_y_luego_corta(monkeypatch):
    monkeypatch.setattr(ratelimit, "_local", {})
    user = f"u-{uuid.uuid4()}"
    results = [await ratelimit.hit_daily(user, limit=3) for _ in range(4)]
    assert [ok for ok, _ in results] == [True, True, True, False]
    # El segundo devuelto es el tiempo hasta el próximo día UTC (siempre dentro de un día).
    assert 0 < results[-1][1] <= 86400


@pytest.mark.asyncio
async def test_el_tope_diario_es_independiente_de_cada_usuario(monkeypatch):
    monkeypatch.setattr(ratelimit, "_local", {})
    a = f"a-{uuid.uuid4()}"
    b = f"b-{uuid.uuid4()}"
    await ratelimit.hit_daily(a, limit=1)
    ok_a, _ = await ratelimit.hit_daily(a, limit=1)
    ok_b, _ = await ratelimit.hit_daily(b, limit=1)
    assert ok_a is False
    assert ok_b is True


@pytest.mark.asyncio
async def test_el_tope_diario_no_depende_del_limite_por_minuto(monkeypatch):
    monkeypatch.setattr(ratelimit, "_local", {})
    user = f"u-{uuid.uuid4()}"
    await ratelimit.hit(user, limit=1, scope="chat")
    ok_day, _ = await ratelimit.hit_daily(user, limit=2)
    assert ok_day is True
