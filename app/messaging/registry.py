"""Valida sobre y `data` contra los esquemas de festa-infra/contracts (única fuente de verdad)."""
import json
from functools import lru_cache
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker


class SchemaRegistry:
    def __init__(self, contracts_dir: str):
        root = Path(contracts_dir)
        checker = FormatChecker()
        self.envelope = Draft202012Validator(json.loads((root / "envelope.schema.json").read_text("utf-8")), format_checker=checker)
        self.data: dict[str, Draft202012Validator] = {}
        self.producers: dict[str, str] = {}
        self.exchanges: dict[str, str] = {}
        for file in (root / "events").glob("*/*.schema.json"):
            schema = json.loads(file.read_text("utf-8"))
            key = schema["x-routing-key"]
            self.data[key] = Draft202012Validator(schema, format_checker=checker)
            self.producers[key] = schema["x-producer"]
            self.exchanges[key] = schema["x-exchange"]

    def envelope_errors(self, envelope) -> list[str]:
        return [e.message for e in self.envelope.iter_errors(envelope)]

    def data_errors(self, event_type: str, data) -> list[str]:
        validator = self.data.get(event_type)
        return [e.message for e in validator.iter_errors(data)] if validator else []


@lru_cache(maxsize=4)
def get_registry(contracts_dir: str) -> SchemaRegistry:
    return SchemaRegistry(contracts_dir)
