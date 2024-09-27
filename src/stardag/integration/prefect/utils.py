import re


def format_key(key: str) -> str:
    """Only allow lowercase letters, numbers, and dashes.
    All other characters are replaced with dashes.
    """
    return re.sub(r"[^a-z0-9-]", "-", key.lower())
