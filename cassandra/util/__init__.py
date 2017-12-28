def int_or_str(s):
    try:
        return int(s)
    except ValueError:
        return s
