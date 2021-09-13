if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Fetch file metadata from the Google Drive API."
    )
    parser.add_argument("input", help="File with one Drive ID per line")
    parser.add_argument(
        "output",
        help="JSON file to store fetched data (in normal mode) or output directory (in dl mode)",
    )
    parser.add_argument(
        "--dl", action="store_true", help="Recursively download files and metadata"
    )
    parser.add_argument(
        "--follow-shortcuts", action="store_true", help="Follow shortcuts in dl mode"
    )
    parser.add_argument(
        "--follow-parents", action="store_true", help="Follow parent folders in dl mode"
    )
    parser.add_argument(
        "--restore",
        default=None,
        type=str,
        help="Path to DB for restoring from failed metadata fetch",
    )
    parser.add_argument(
        "--fields",
        default=None,
        type=str,
        help=(
            "(default: the default fields returned by the API) "
            "For performance, only request fields you need. "
        ),
    )
    parser.add_argument(
        "--quota",
        default=100,
        type=int,
        help="(default: %(default)s) Max queries per second",
    )
    parser.add_argument(
        "--concurrent",
        metavar="N",
        default=100,
        type=int,
        help="(default: %(default)s) Max concurrent queries",
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
        default="token.pickle",
        help="(default: %(default)s) File to store access and refresh token in",
    )
    args = parser.parse_args()

    if args.concurrent > args.quota:
        print(
            f"`concurrent` ({args.concurrent}) must be <= `quota` ({args.quota}). "
            "Setting `concurrent` to `quota`."
        )
        args.concurrent = args.quota


import asyncio
import json
import os
import pickle

from aiogoogle import Aiogoogle
from aiogoogle.auth.creds import ClientCreds, UserCreds
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from tqdm import tqdm

import dl
from rate_limit import rate_limited_as_completed
from util import ErrorTracker

# See all scopes at: https://developers.google.com/drive/api/v3/about-auth#OAuth2Scope
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]


# We could do this in Aiogoogle, but having to setup an async web server is
# annoying. It's easier to let Google's library handle this for us. There's no
# gain to performing authorization asynchronously, anyway.
def get_creds(credentials_file, token_file, host, port):
    creds = None
    if os.path.exists(token_file):
        with open(token_file, "rb") as f:
            creds = pickle.load(f)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(credentials_file, SCOPES)
            creds = flow.run_local_server(host=host, port=port)

        with open(token_file, "wb") as f:
            pickle.dump(creds, f)

    # Here are all the common attributes between google.oauth2.credentials.Credentials
    # and aiogoogle.auth.creds.UserCreds. UserCreds has more attributes, but
    # I'm guessing they're not required.
    user_creds = UserCreds(
        access_token=creds.token,
        refresh_token=creds.refresh_token,
        expires_at=creds.expiry.isoformat(),
        scopes=creds.scopes,
        id_token=creds.id_token,
        token_uri=creds.token_uri,
    )
    # The ID and secret are needed for refreshing the token
    client_creds = ClientCreds(
        client_id=creds.client_id,
        client_secret=creds.client_secret,
    )
    return user_creds, client_creds


async def get_metadata(aiogoogle, drive, ids, fields, max_concurrent, quota):
    metadata = []
    err_track = ErrorTracker(args.indent)
    pbar = tqdm(total=len(ids), unit="req")
    coros = [aiogoogle.as_user(drive.files.get(fileId=id, fields=fields)) for id in ids]
    # coros = [aiogoogle.as_user(drive.files.get(fileId=id, download_file="test-path.data", alt="media", validate=False)) for id in ids]
    for coro in rate_limited_as_completed(coros, max_concurrent, quota):
        res = await err_track(coro)
        if res:
            metadata.append(res)
        pbar.update(1)

    pbar.close()
    return metadata, err_track


async def main(args):
    with open(args.input) as f:
        ids = list(set(filter(None, map(lambda i: i.strip(), f.readlines()))))

    user_creds, client_creds = get_creds(
        args.credentials, args.token, args.host, args.port
    )
    async with Aiogoogle(user_creds=user_creds, client_creds=client_creds) as aiogoogle:
        drive = await aiogoogle.discover("drive", "v3")
        if args.dl:
            err_track = await dl.main(ids, aiogoogle, drive, args)
        else:
            metadata, err_track = await get_metadata(
                aiogoogle, drive, ids, args.fields, args.concurrent, args.quota
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
