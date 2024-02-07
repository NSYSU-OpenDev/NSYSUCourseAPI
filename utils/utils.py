def is_integer(text: str) -> bool:
    """
    Check if the text conforms to int.

    Args:
        text (str): The string to check.

    Returns:
        bool: Whether the text conforms to int.
    """
    try:
        int(text)
        return True
    except ValueError:
        return False
