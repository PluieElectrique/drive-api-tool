if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Fetch file metadata from 'Shared With Me'."
    )
    parser.add_argument(
        "output",
        help="JSON file to store fetched data",
    )
    parser.add_argument(
        "--fields",
        # For some reason, files.list doesn't return any fields by default.
        # These are the default fields returned by the API tester.
        default="kind,id,name,mimeType",
        type=str,
        help=("(default: %(default)s) For performance, only request fields you need."),
    )
    parser.add_argument(
        "--quota",
        default=100,
        type=int,
        help="(default: %(default)s) Max queries per second",
    )
    parser.add_argument(
        "--indent",
        default=None,
        type=int,
        help="(default: %(default)s) Spaces to indent JSON by",
    )
    parser.add_argument(
        "--host",
        default="localhost",
        help="(default: %(default)s) Host of local auth server",
    )
    parser.add_argument(
        "--port",
        default=8000,
        type=int,
        help="(default: %(default)s) Port of local auth server",
    )
    parser.add_argument(
        "--credentials",
        metavar="CREDS",
        default="credentials.json",
        help="(default: %(default)s)",
    )
    parser.add_argument(
        "--token",
        default="token.json",
        help="(default: %(default)s) File to store access and refresh token in",
    )
    args = parser.parse_args()


import asyncio
from datetime import datetime
import json
import logging
import time

from aiogoogle import Aiogoogle
from tqdm import tqdm

from drive_api_tool import get_creds
from util import ErrorTracker


now = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
logger_filename = f"drive_shared_with_me_errors_{now}.log"
print(f"Logging errors to {logger_filename}")
logging.basicConfig(filename=logger_filename)
logger = logging.getLogger(__name__)


async def shared_with_me(aiogoogle, drive, fields, quota, indent):
    metadata = []
    err_track = ErrorTracker(logger, indent)
    pbar = tqdm(desc="Fetching 'Shared With Me' files", unit="req")

    # pageSize is the number of results to return per request. It must be
    # between 1 and 1000, inclusive. I assume that the biggest page size means
    # the fewest requests and so the fastest speed.
    def make_coro(page_token=None, page_size=1000):
        return aiogoogle.as_user(
            drive.files.list(
                q="sharedWithMe",
                fields=f"nextPageToken,incompleteSearch,files({fields})",
                pageToken=page_token,
                pageSize=page_size,
            )
        )

    coro = make_coro()
    while True:
        now = time.monotonic()
        res = await err_track(coro)
        pbar.update(1)
        if not res:
            break

        metadata.extend(res["files"])

        if res["incompleteSearch"]:
            print("Warning: incomplete search. Some files may be missing.")

        next_page_token = res.get("nextPageToken")
        if next_page_token is None:
            break

        coro = make_coro(page_token=next_page_token)

        elapsed = time.monotonic() - now
        if elapsed < 1 / quota:
            # No MIN_SLEEP, it's probably fine
            await asyncio.sleep(1 / quota - elapsed)

    pbar.close()
    return metadata, err_track


async def main(args):
    user_creds, client_creds = get_creds(
        args.credentials, args.token, args.host, args.port
    )
    async with Aiogoogle(user_creds=user_creds, client_creds=client_creds) as aiogoogle:
        drive = await aiogoogle.discover("drive", "v3")
        metadata, err_track = await shared_with_me(
            aiogoogle, drive, args.fields, args.quota, args.indent
        )

        with open(args.output, "w") as f:
            json.dump(
                {"metadata": metadata, "errors": err_track.errors},
                f,
                indent=args.indent,
            )

    err_track.print_errors()


if __name__ == "__main__":
    asyncio.run(main(args))
