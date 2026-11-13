import json
import os
import tempfile
from pathlib import Path

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

_KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)
PRIVATE_PEM = _KEY.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption())
_pub = Path(tempfile.mkdtemp()) / "jwt_public.pem"
_pub.write_bytes(_KEY.public_key().public_bytes(serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo))

TEST_MONGO_URL = os.environ.get("TEST_MONGO_URL")
os.environ.setdefault("MONGO_URL", TEST_MONGO_URL or "mongodb://localhost:27017/ai_test")
os.environ.setdefault("AMQP_URL", "amqp://test:test@localhost:5672")
os.environ.setdefault("JWT_ISSUER", "festa-auth")
os.environ["JWT_PUBLIC_KEY_PATH"] = str(_pub)
os.environ["LLM_PROVIDER"] = "fake"
os.environ["LLM_API_KEY"] = ""
os.environ["REDIS_URL"] = ""
os.environ.setdefault("FESTA_CONTRACTS_DIR", str(Path("../festa-infra/contracts").resolve()))

CONTRACTS = Path(os.environ["FESTA_CONTRACTS_DIR"])
HAS_CONTRACTS = (CONTRACTS / "envelope.schema.json").exists()


def token(sub: str, role: str = "user") -> str:
    import jwt

    return jwt.encode({"sub": sub, "role": role, "iss": "festa-auth", "exp": 4_102_444_800}, PRIVATE_PEM, algorithm="RS256")


def contract_example(domain: str, name: str) -> dict:
    return json.loads((CONTRACTS / "events" / domain / f"{name}.schema.json").read_text("utf-8"))["examples"][0]


def validate_contract(domain: str, name: str, payload: dict) -> list[str]:
    from jsonschema import Draft202012Validator, FormatChecker

    schema = json.loads((CONTRACTS / "events" / domain / f"{name}.schema.json").read_text("utf-8"))
    return [e.message for e in Draft202012Validator(schema, format_checker=FormatChecker()).iter_errors(payload)]


@pytest.fixture
async def mongo_db():
    if not TEST_MONGO_URL:
        pytest.skip("sin TEST_MONGO_URL")
    from pymongo import AsyncMongoClient

    from app import db as dbmod

    client = AsyncMongoClient(TEST_MONGO_URL, tz_aware=True)
    database = client.get_default_database()
    await database.client.drop_database(database.name)
    await dbmod.ensure_indexes(database)
    dbmod.set_db_for_tests(database)
    yield database
    await database.client.drop_database(database.name)
    dbmod.set_db_for_tests(None)
    await client.close()
