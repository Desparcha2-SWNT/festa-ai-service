"""Supresión de cuentas (Ley 1581): lápida, rechazo de usuarios eliminados y cancelación de mensajes.

Sin transacciones (Mongo sin réplica), una escritura que compite con `account.deleted` sigue el
patrón «escribir y volver a mirar la lápida»: `account.deleted` escribe la lápida ANTES de borrar,
y quien escribe datos personales la consulta DESPUÉS de escribir. En cualquier intercalado, o el
borrado ve lo escrito y lo elimina, o quien escribió ve la lápida y deshace su propia escritura.
"""
from fastapi import Depends, HTTPException

from .auth import Principal, current_user
from .db import get_db


async def is_deleted(db, user_id: str | None) -> bool:
    return bool(user_id) and await db.deleted_accounts.find_one({"_id": user_id}) is not None


async def cancel_unsent(db, user_id: str, event_ids: list[str] | None = None) -> int:
    """Cancela mensajes aún no publicados que llevan el id de la persona.

    `recommendation.served` (userId) y `description.generated` (requestedBy) exigen ese id en el
    contrato, así que no se pueden sanear: se cancelan. Los ya publicados los atiende cada consumidor
    con su propio manejo de `account.deleted`.
    """
    query = {"sentAt": None, "$or": [{"payload.userId": user_id}, {"payload.requestedBy": user_id}]}
    if event_ids is not None:
        query["_id"] = {"$in": event_ids}
    result = await db.outbox.delete_many(query)
    return result.deleted_count


# Identificador anónimo con formato uuid: el contrato exige userId/requestedBy y la copia guardada lo conserva.
ANONYMOUS_ID = "00000000-0000-0000-0000-000000000000"


async def anonymize_sent(db, user_id: str) -> int:
    """Quita el id de la persona de las copias ya publicadas que guarda el outbox.

    Lo publicado lo borra cada consumidor con su propio `account.deleted`, pero la copia de este servicio
    seguiría con el id. Se reemplaza por el id anónimo (el payload sigue cumpliendo el contrato) y, además,
    las filas enviadas expiran a los 30 días (índice TTL sobre `sentAt`, ver db.py).
    """
    total = 0
    for field in ("userId", "requestedBy"):
        result = await db.outbox.update_many(
            {"sentAt": {"$ne": None}, f"payload.{field}": user_id}, {"$set": {f"payload.{field}": ANONYMOUS_ID}}
        )
        total += result.modified_count
    return total


async def active_user(user: Principal = Depends(current_user)) -> Principal:
    """El JWT sigue vigente un rato después de borrar la cuenta: se rechaza con la lápida."""
    if await is_deleted(get_db(), user.user_id):
        raise HTTPException(status_code=403, detail="la cuenta fue eliminada")
    return user
