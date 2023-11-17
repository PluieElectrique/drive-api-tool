import argparse
import asyncio
from datetime import datetime
import logging
import os
import sqlite3

from aiogoogle import Aiogoogle
from aiogoogle.auth.creds import ClientCreds, UserCreds
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from tqdm import tqdm

from export_config import WORKSPACE_EXPORT
from rate_limit import rate_limited_as_completed
from util import ErrorTracker, sanitize_filename


# https://developers.google.com/drive/api/v3/reference/about/get
WORKSPACE_MIME_TYPES = [
    "application/vnd.google-apps.document",
    "application/vnd.google-apps.drawing",
    "application/vnd.google-apps.form",
    "application/vnd.google-apps.jam",
    "application/vnd.google-apps.mail-layout",
    "application/vnd.google-apps.presentation",
    "application/vnd.google-apps.script",
    "application/vnd.google-apps.site",
    "application/vnd.google-apps.spreadsheet",
]

# https://developers.google.com/drive/api/v3/ref-export-formats
# https://developers.google.com/drive/api/v3/reference/about/get
WORKSPACE_EXPORT_MIME_EXTENSION = {
    "application/epub+zip": ".epub",
    "application/pdf": ".pdf",
    "application/rtf": ".rtf",
    "application/vnd.google-apps.script+json": ".json",
    "application/vnd.oasis.opendocument.presentation": ".odp",
    "application/vnd.oasis.opendocument.spreadsheet": ".ods",
    "application/vnd.oasis.opendocument.text": ".odt",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation": ".pptx",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
    "application/x-vnd.oasis.opendocument.spreadsheet": ".ods",
    "application/zip": ".zip",
    "image/jpeg": ".jpeg",
    "image/png": ".png",
    "image/svg+xml": ".svg",
    "text/csv": ".csv",
    "text/html": ".html",
    "text/plain": ".txt",
    "text/tab-separated-values": ".tsv",
}
WORKSPACE_EXPORT_MIME_EXTENSION_MAX_LEN = max(
    len(e) for e in WORKSPACE_EXPORT_MIME_EXTENSION.values()
)

# See all scopes at: https://developers.google.com/drive/api/v3/about-auth#OAuth2Scope
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

# We could do this in Aiogoogle, but having to setup an async web server is
# annoying. It's easier to let Google's library handle this for us. There's no
# gain to performing authorization asynchronously, anyway.
def get_creds(credentials_file, token_file, host, port):
    creds = None
    if os.path.exists(token_file):
        creds = Credentials.from_authorized_user_file(token_file, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except Exception as e:
                print("Failed to refresh credentials:", e)
                flow = InstalledAppFlow.from_client_secrets_file(
                    credentials_file, SCOPES
                )
                creds = flow.run_local_server(host=host, port=port)
        else:
            flow = InstalledAppFlow.from_client_secrets_file(credentials_file, SCOPES)
            creds = flow.run_local_server(host=host, port=port)

        with open(token_file, "w") as f:
            f.write(creds.to_json())

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
        scopes=creds.scopes,                # NOTE: This is new, is it needed?
    )
    return user_creds, client_creds


class Db:
    def __init__(self, filename):
        self.conn = sqlite3.connect(filename)
        self.c = self.conn.cursor()

    def files_to_download(self):
        work_mimes = set(WORKSPACE_MIME_TYPES)
        work_mime_counts = { k: len(v) for k, v in WORKSPACE_EXPORT.items() }

        non_workdoc = 0
        workdocs = 0
        workdoc_export_total = 0

        print("Counting how many files need to be downloaded...")
        self.c.execute("SELECT COUNT(*), mime_type FROM data GROUP BY mime_type")

        for count, mime in self.c:
            if mime in work_mimes:
                workdocs += count
                workdoc_export_total += count * work_mime_counts[mime]
            else:
                non_workdoc += count

        print("Total files to download:", non_workdoc + workdoc_export_total)
        print("  Total non-workspace files to download:", non_workdoc)
        print("  Total workspace files to export      :", workdoc_export_total)
        print("    Total workspace file IDs           :", workdocs)
        print("    Workspace exports / workspace IDs  :", f"{workdoc_export_total/workdocs:.3f}x" if workdocs > 0 else "N/A")
        print()
        return non_workdoc + workdoc_export_total

    def read(self, chunk_size=10000):
        self.c.execute("SELECT COUNT(*) FROM data")
        id_total = self.c.fetchone()[0]
        for offset in range(0, id_total, chunk_size):
            self.c.execute(f"SELECT * FROM data LIMIT {chunk_size} OFFSET {offset}")
            yield self.c.fetchall()

    def close(self):
        self.c.close()
        self.conn.close()


def filename(id, name, extension, mime_type, version, forbidden_sub=None):
    """Filename for saving a file to disk."""
    # Exported document:   {name}_{id}_{version}  (no extension but leave space for it)
    # File with extension: {name w/o extension}_{id}.{extension}
    # File w/o extension:  {name}_{id}
    #
    # If the entire filename is too long, {name} is truncated until it fits.
    #
    # The version number is used to track file changes, but it often
    # changes for no apparent reason. For example, an unmodified raw file
    # may go through hundreds of version numbers if downloaded over a
    # period of time. The API docs note that:
    #   [The version number] reflects every change made to the file on the
    #   server, even those not visible to the user.
    #
    # This isn't too much of a problem for docs, since they're usually
    # small. But, duplicate raw files are bad when the files are large.
    # Also, raw files have hashes, which should really be used instead.

    reserved_space = 0
    if mime_type in WORKSPACE_MIME_TYPES:
        # XXX: Actually, this is kind of dangerous, because if we have fixed metadata, then we will never download the
        # newest version of a workdoc. Also, if the file is updated after metadata is fetched but before download, then
        # the filename will actually have the wrong version.
        suffix = f"_{id}_{version}"
        reserved_space += WORKSPACE_EXPORT_MIME_EXTENSION_MAX_LEN
    elif extension is not None and extension != "":
        # The "fullFileExtension" field "is not cleared if the new name does not contain a valid extension."
        # (https://developers.google.com/drive/api/v3/reference/files)
        if name.endswith(extension):
            # name already has extension, so remove it and add at end
            suffix = f"_{id}.{extension}"
            name = name[: -(1 + len(extension))]
        else:
            # Here there are 2 cases:
            #   1. `name` has no extension: So we want to use Drive's.
            #   2. `name` has a different extension: Keep both extensions but assume Drive is right, so {name with ext}_{id}.{extension}
            suffix = f"_{id}.{extension}"
    else:
        suffix = f"_{id}"

    reserved_space += len(suffix)
    filename = sanitize_filename(name, reserved_space, forbidden_sub)
    return filename + suffix

async def wrap_coro(size, path, coro):
    return size, path, await coro

def should_download(path, name, size, logger):
    if os.path.exists(path):
        if size is not None:
            size = int(size)
            downloaded_size = os.path.getsize(path)
            if size != downloaded_size:
                arrow = "<" if downloaded_size < size else ">"
                logger.warn(f"Previously downloaded file '{name}' ({id}): size {downloaded_size} {arrow} expected size {size}")
                return not args.no_size_redownload
        # If we don't have a size, then assume the file downloaded correctly.
        return False
    else:
        return True

def get_stuff(aiogoogle, drive, db, pbar, out_dir, chunk_size, logger):
    coros = []
    for chunk in db.read():
        for id, name, extension, mime_type, size, resource_key, version in chunk:
            path_dir = os.path.join(out_dir, id[:2], id[2:4])
            os.makedirs(path_dir, exist_ok=True)
            path = os.path.join(path_dir, filename(id, name, extension, mime_type, version))

            resource_key = f"{id}/{resource_key}" if resource_key else None

            if mime_type in WORKSPACE_MIME_TYPES:
                for export_mime in WORKSPACE_EXPORT[mime_type]:
                    ext = WORKSPACE_EXPORT_MIME_EXTENSION[export_mime]
                    # We don't know the size of an exported file, so we assume that a file means downloaded
                    if os.path.exists(path + ext):
                        pbar.update(1)
                    else:
                        coros.append(aiogoogle.as_user(
                            drive.files.export(
                                fileId=id,
                                mimeType=export_mime,
                                download_file=path + ext,
                                id_resource_key=resource_key,
                                validate=False,
                            )
                        ))
            else:
                if should_download(path, name, size, logger):
                    coros.append(wrap_coro(size, path, aiogoogle.as_user(
                        drive.files.get(
                            fileId=id,
                            download_file=path,
                            download_file_size=None if size is None else int(size),
                            id_resource_key=resource_key,
                            alt="media",
                            validate=False,
                        )
                    )))
                else:
                    pbar.update(1)

            if len(coros) > chunk_size:
                yield coros
                coros = []

    if coros:
        yield coros


async def main(args, logger):
    user_creds, client_creds = get_creds(args.credentials, args.token, args.host, args.port)

    async with Aiogoogle(user_creds=user_creds, client_creds=client_creds) as aiogoogle:
        drive = await aiogoogle.discover("drive", "v3")

        db = Db(args.db)
        err_track = ErrorTracker(logger, args.indent)
        pbar = tqdm(total=db.files_to_download(), desc="Download files", unit="item")

        for chunk in get_stuff(aiogoogle, drive, db, pbar, args.out_dir, args.download_concurrent * 100, logger):
            for coro in rate_limited_as_completed(chunk, args.download_concurrent, args.quota):
                res = await err_track(coro)
                if isinstance(res, tuple):
                    size, path, _ = res
                    if size is not None:
                        size = int(size)
                        downloaded_size = os.path.getsize(path)
                        if downloaded_size != size:
                            arrow = "<" if downloaded_size < size else ">"
                            logger.warn(f"File '{path}' ({id}) just downloaded, but size {downloaded_size} {arrow} expected size {size}")
                pbar.update(1)

        pbar.close()
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download files")
    parser.add_argument("db", help="DB from 01_extract.py")
    parser.add_argument("out_dir", help="Output directory")
    parser.add_argument("--no-size-redownload", help="Don't redownload files when downloaded size != metadata size", action="store_true")
    parser.add_argument("--quota", default=100, type=int, help="(default: %(default)s) Max queries per second")
    parser.add_argument("--download-concurrent", metavar="N", default=25, type=int,
        help="(default: %(default)s) Max concurrent queries for downloading files")
    parser.add_argument("--indent", default=None, type=int, help="(default: %(default)s) Spaces to indent error JSON by")
    parser.add_argument("--host", default="localhost", help="(default: %(default)s) Host of local auth server")
    parser.add_argument("--port", default=8000, type=int, help="(default: %(default)s) Port of local auth server")
    parser.add_argument("--credentials", metavar="CREDS", default="credentials.json", help="(default: %(default)s)")
    parser.add_argument("--token", default="token.json", help="(default: %(default)s) File to store access and refresh token in")
    args = parser.parse_args()

    now = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    logger_filename = f"simple_dl_02_download_errors_{now}.log"
    print(f"Logging to {logger_filename}\n")
    logging.basicConfig(filename=logger_filename)
    logger = logging.getLogger(__name__)

    asyncio.run(main(args, logger))
