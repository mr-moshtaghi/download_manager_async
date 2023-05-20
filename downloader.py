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
    async with aiofiles.tempfile.TemporaryDirectory():
        download_future: asyncio.Future = asyncio.gather(
            [
                # download_
            ]
        )






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
