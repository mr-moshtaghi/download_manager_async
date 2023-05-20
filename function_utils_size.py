from math import ceil


def sizeof_fmt(
        num: int,
        suffix: str = "B"
) -> str:
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"


def calc_file_chunks(
        file_size: int,
        min_chunk_size: int = None,
        max_chunk_size: int = None,
) -> tuple:
    if min_chunk_size is None:
        min_chunk_size = 10 * 1024 * 1024  # 10 MB

    if max_chunk_size is None:
        max_chunk_size = 100 * 1024 * 1024  # 100 MB

    default_total_parts = 3
    splitted_parts = []

    while (
            ceil(file_size / min_chunk_size) < default_total_parts and
            default_total_parts > 1
    ):
        default_total_parts -= 1

    while (
            ceil(file_size / max_chunk_size) > default_total_parts and
            default_total_parts < 6
    ):
        default_total_parts += 1

    file_total_size = default_total_parts
    if file_total_size < 1:
        file_total_size = 1

    chunk = ceil(file_size / file_total_size)

    for i in range(file_total_size):
        start_byte = i * chunk
        end_byte = start_byte + chunk
        if end_byte > file_size:
            end_byte = file_size
        end_byte -= 1
        splitted_parts.append((start_byte, end_byte))

    return splitted_parts, chunk


def verify_splitted_chunks(
        parts: list,
        file_size: int
) -> bool:
    calc_size = sum(map(lambda part: part[1] - part[0] + 1, parts))
    assert calc_size == file_size, "file size mismatch"
    return True

