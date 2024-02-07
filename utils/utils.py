def is_integer(str: str) -> bool:
    """
    Check if the text conforms to int.

    :param str: The string to check. (str)
    :return: bool
    """
    try:
        int(str)
        return True
    except ValueError:
        return False
