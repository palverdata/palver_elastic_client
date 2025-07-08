from typing import Optional


def drop_none_keys(lst: list[dict] | dict) -> list[dict] | dict:
    if isinstance(lst, list):
        return [{k: v for k, v in d.items() if v is not None} for d in lst]
    elif isinstance(lst, dict):
        return {k: v for k, v in lst.items() if v is not None}
    else:
        raise TypeError("Input must be a dictionary or a list of dictionaries")


def drop_none_keys_and_keep(
    lst: list[dict] | dict, keep: Optional[list[str]] = None
) -> list[dict] | dict:
    if isinstance(lst, list):
        return [{k: v for k, v in d.items() if v is not None or k in keep} for d in lst]
    elif isinstance(lst, dict):
        return {k: v for k, v in lst.items() if v is not None or k in keep}
    else:
        raise TypeError("Input must be a dictionary or a list of dictionaries")


def drop_empty_arrays(lst: list[dict] | dict) -> list[dict] | dict:
    if isinstance(lst, list):
        return [{k: v for k, v in d.items() if v != []} for d in lst]
    elif isinstance(lst, dict):
        return {k: v for k, v in lst.items() if v != []}
    else:
        raise TypeError("Input must be a dictionary or a list of dictionaries")


def drop_zeros(lst: list[dict] | dict) -> list[dict] | dict:
    if isinstance(lst, list):
        return [{k: v for k, v in d.items() if v != 0} for d in lst]
    elif isinstance(lst, dict):
        return {k: v for k, v in lst.items() if v != 0}
    else:
        raise TypeError("Input must be a dictionary or a list of dictionaries")


def omit(d: dict, keys: list[str]) -> dict:
    """Omit keys from a dictionary."""
    return {k: v for k, v in d.items() if k not in keys}
