"""Identificadores: validación de UUID completo y forma canónica (minúsculas) para guardar y emitir."""

# UUID completo (8-4-4-4-12 hexadecimales), no solo "36 caracteres hexadecimales o guiones".
UUID_PATTERN = r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"


def canonical(value: str) -> str:
    """Forma única de un UUID: la misma petición en mayúsculas o minúsculas debe caer en el mismo registro."""
    return value.lower()
