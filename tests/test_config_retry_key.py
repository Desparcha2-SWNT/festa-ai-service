import pytest

from app.config import Settings


def _with_key(monkeypatch, value):
    if value is None:
        monkeypatch.delenv("INBOX_RETRY_KEY", raising=False)
    else:
        monkeypatch.setenv("INBOX_RETRY_KEY", value)


@pytest.mark.parametrize("value", [None, "", "corta", "change-me-" + "x" * 20, "ChangeMe" + "x" * 20])
def test_clave_de_reintentos_ausente_corta_o_de_ejemplo_detiene_el_arranque(monkeypatch, value):
    _with_key(monkeypatch, value)
    with pytest.raises(RuntimeError, match="INBOX_RETRY_KEY"):
        Settings()


def test_clave_de_reintentos_real_se_acepta(monkeypatch):
    _with_key(monkeypatch, "a" * 64)
    assert Settings().inbox_retry_key == "a" * 64
