"""Configuración por variables de entorno (las pone el docker compose de festa-infra)."""
import os
from dataclasses import dataclass, field
from pathlib import Path


def _required(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Falta la variable de entorno {name}")
    return value


@dataclass(frozen=True)
class Settings:
    service_name: str = "ai-service"
    port: int = int(os.environ.get("PORT", "8086"))
    log_level: str = os.environ.get("LOG_LEVEL", "info")
    mongo_url: str = field(default_factory=lambda: _required("MONGO_URL"))
    redis_url: str = field(default_factory=lambda: os.environ.get("REDIS_URL", ""))
    amqp_url: str = field(default_factory=lambda: _required("AMQP_URL"))
    jwt_issuer: str = field(default_factory=lambda: _required("JWT_ISSUER"))
    jwt_public_key_path: str = field(default_factory=lambda: _required("JWT_PUBLIC_KEY_PATH"))
    contracts_dir: str = os.environ.get(
        "FESTA_CONTRACTS_DIR", str(Path("../festa-infra/contracts").resolve())
    )
    # openai | gemini | anthropic | ollama | fake. Sin LLM_API_KEY (salvo ollama) se usa "fake":
    # un generador local sin costo, útil en desarrollo y pruebas.
    llm_provider: str = os.environ.get("LLM_PROVIDER", "gemini").lower()
    llm_api_key: str = os.environ.get("LLM_API_KEY", "")
    # RNF8: modelos de bajo costo (nivel "mini"/"flash").
    llm_chat_model: str = os.environ.get("LLM_CHAT_MODEL", "gemini-2.5-flash")
    llm_description_model: str = os.environ.get("LLM_DESCRIPTION_MODEL", "gemini-2.5-flash")
    ollama_url: str = os.environ.get("OLLAMA_URL", "http://ollama:11434")
    llm_timeout_s: float = float(os.environ.get("LLM_TIMEOUT_SECONDS", "30"))
    # RNF8: límite de mensajes de chat por usuario por minuto (5–20).
    chat_rate_limit_per_min: int = int(os.environ.get("AI_CHAT_RATE_LIMIT_PER_MIN", "10"))
    # Cuántos eventos candidatos se le pasan al modelo en cada turno.
    chat_max_candidates: int = int(os.environ.get("AI_CHAT_MAX_CANDIDATES", "25"))
    chat_history_turns: int = int(os.environ.get("AI_CHAT_HISTORY_TURNS", "8"))

    @property
    def effective_provider(self) -> str:
        if self.llm_provider == "ollama":
            return "ollama"
        # "none" es lo que pone el Terraform del lab cuando no hay llave (los secretos de ECS no pueden ir vacíos).
        if self.llm_api_key.strip().lower() in ("", "none") or self.llm_provider == "fake":
            return "fake"
        return self.llm_provider


_settings: Settings | None = None


def get_settings() -> Settings:
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings


def reset_settings() -> None:
    global _settings
    _settings = None
