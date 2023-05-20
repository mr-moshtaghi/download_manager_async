import argparse
import asyncio
import aiohttp
import os
from math import ceil
import datetime
import aiofiles
from aiofiles.os import remove as os_remove

from function_utils_size import sizeof_fmt, calc_file_chunks, verify_splitted_chunks


async def head_request(
        url: str
) -> dict:
    async with aiohttp.ClientSession() as session:
        async with session.head(url, allow_redirects=True) as response:
            return dict(response.headers)


async def download_part(
        url: str,
        temp_dir: str,
        part_id: int,
        start_byte: int,
        end_byte: int,
        queue: asyncio.Queue[int],
) -> tuple:
    headers = {"Range": f"bytes={start_byte}-{end_byte}"}
    file_name = url.split("/")[-1]
    timeout = aiohttp.ClientTimeout(connect=6 * 60)
    async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
        async with session.get(url=url) as response:
            file_path = os.path.join(temp_dir, file_name + f".part{part_id}")
            async with aiofiles.open(file_path, "wb") as afw:
                while True:
                    chunk = await response.content.read(10 * 1024 * 1024)
                    if not chunk:
                        break
                    await afw.write(chunk)
                    await queue.put(len(chunk))
    await queue.put(-1)


async def delete_file(
        file: str,
        temp_dir: str = None,
) -> None:
    file_path = os.path.join(temp_dir, file) if temp_dir is not None else file
    if os.path.exists(file_path):
        await os_remove(file_path)


async def merge_file_parts(
        file_name: str,
        temp_dir: str,
        part_ids: list
) -> bool:
    async with aiofiles.open(file_name, "wb") as afw:
        for part_id in part_ids:
            file_part = file_name + f".part{part_id}"
            file_path = os.path.join(temp_dir, file_part)
            async with aiofiles.open(file_path, "rb") as afr:
                while True:
                    chunk = await afr.read(10 * 1024 * 1024)
                    if not chunk:
                        break
                    await afw.write(chunk)
            await delete_file(file_part, temp_dir)
    return True


async def show_download_progress(
        queue: asyncio.Queue[int],
        total_size: int,
        total_parts: int
) -> None:
    downloaded = 0
    finished = 0

    while finished != total_parts:
        download_per_second = 0
        while not queue.empty():
            chunk_size = await queue.get()
            if chunk_size != -1:
                finished += 1
            download_per_second += chunk_size

        downloaded += download_per_second
        if download_per_second > 0:
            completed_count = ceil(downloaded / total_size * 50)
            remaining_second = (total_size - downloaded) // download_per_second
            display = "{downloaded:7s} ({percent}%) {progressed}{remaining} {speed}/s {eta}".format(
                downloaded=sizeof_fmt(downloaded),
                percent=ceil(downloaded / total_size * 100),
                progressed="=" * completed_count,
                remaining="-" * (50 - completed_count),
                speed=sizeof_fmt(download_per_second),
                eta=str(datetime.timedelta(seconds=remaining_second)),
            )
            end_display = "\r" if finished != total_parts else "\n"
            print(display, end=end_display)
        await asyncio.sleep(1)


async def download_file(
        url: str,
        min_chunk_size: int = None,
        max_chunk_size: int = None,
        output: str = None
) -> bool:
    try:
        headers = await head_request(url)
    except KeyboardInterrupt:
        print("download cancelled")

    content_length = headers.get("Content-Length", None)
    if content_length is None:
        raise Exception("url file has no Content-Length header")

    file_name = url.split("/")[-1]  # os.path.basename(url)
    file_size = int(content_length)
    print(f"File size : {file_size} ({sizeof_fmt(file_size)})")

    splitted_parts, chunk_size = calc_file_chunks(
        file_size, min_chunk_size, max_chunk_size
    )

    file_total_parts = len(splitted_parts)

    verify_splitted_chunks(splitted_parts, file_size)

    print(
        f"file total parts {file_total_parts} each part is almost {sizeof_fmt(chunk_size)}"
    )

    queue: asyncio.Queue[int] = asyncio.Queue(100)
    async with aiofiles.tempfile.TemporaryDirectory() as temp_dir:
        download_future: asyncio.Future = asyncio.gather(
            *[
                download_part(url, temp_dir, part_id + 1, start, end, queue)
                for part_id, (start, end) in enumerate(splitted_parts)
            ],
            show_download_progress(queue, file_size, file_total_parts),
        )

        print("download started...")

        try:
            await download_future
            downloaded = True
        except Exception:
            downloaded = False

        if downloaded:
            print("merging parts...")
            saved = await merge_file_parts(
                file_name, temp_dir, list(range(1, file_total_parts + 1))
            )
        else:
            print("deleting the partial downloaded part...")
            delete_file_future = asyncio.gather(
                *[
                    delete_file(file_name + f".part{part_id}", temp_dir)
                    for part_id in range(1, file_total_parts + 1)
                ]
            )
            await delete_file_future
    return saved


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="File downloader")
    parser.add_argument("url", metavar="URL", type=str, help="url to download")
    parser.add_argument(
        "--min-chunk-size", type=int, help="minimum file chunk size (default 10MB)"
    )
    parser.add_argument(
        "--max-chunk-size", type=int, help="maximum file chunk size (default 100MB)"
    )
    parser.add_argument("-O", "--output", type=str, help="output file path (default working directory")
    args = parser.parse_args()
    if args.url:
        saved = asyncio.run(
            download_file(
                url=args.url,
                min_chunk_size=args.min_chunk_size,
                max_chunk_size=args.max_chunk_size,
                output=args.output
            )
        )
        if saved:
            print("File download completed")
        else:
            print("File download faild")
