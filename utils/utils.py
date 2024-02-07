def is_integer(str: str) -> bool:
    try:
        int(str)
        return True
    except ValueError:
        return False
