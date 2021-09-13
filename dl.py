WORKSPACE_EXPORT = {
    "application/vnd.google-apps.document": [
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    ],
    "application/vnd.google-apps.spreadsheet": [
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    ],
    "application/vnd.google-apps.drawing": ["image/png"],
    "application/vnd.google-apps.presentation": [
        "application/vnd.openxmlformats-officedocument.presentationml.presentation"
    ],
    "application/vnd.google-apps.script": ["application/vnd.google-apps.script+json"],
}


from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
import json
import os
import sqlite3
import zlib

from tqdm import tqdm

from rate_limit import rate_limited_as_completed
from util import ErrorTracker, sanitize_filename

# Number of results to return per folder contents request (`files.list`). Must
# be between 1 and 1000, inclusive. I assume that the biggest page size means
# the fewest requests and so the fastest speed.
PAGE_SIZE = 1000

WORKSPACE_MIME_TYPES = [
    "application/vnd.google-apps.document",
    "application/vnd.google-apps.drawing",
    "application/vnd.google-apps.presentation",
    "application/vnd.google-apps.script",
    "application/vnd.google-apps.spreadsheet",
]

# https://developers.google.com/drive/api/v3/ref-export-formats
WORKSPACE_EXPORT_MIME_EXTENSION = {
    "application/epub+zip": ".epub",
    "application/pdf": ".pdf",
    "application/rtf": ".rtf",
    "application/vnd.google-apps.script+json": ".json",
    "application/vnd.oasis.opendocument.presentation": ".odp",
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


@dataclass
class Item:
    """The result of fetching a file or folder ID."""

    metadata: dict = None
    is_child: bool = False
    children: list = field(default_factory=list)

    def __getitem__(self, key):
        return self.metadata[key]

    def get(self, key):
        return self.metadata.get(key)

    def is_folder(self):
        return self["mimeType"] == "application/vnd.google-apps.folder"

    def is_workspace_doc(self):
        return self["mimeType"] in WORKSPACE_MIME_TYPES

    def filename(self, forbidden_sub=None):
        """Filename for saving this item to disk."""
        # Folder:              {name}_{id}
        # Exported document:   {name}_{id}_{version}  (no extension but leave space for it)
        # File with extension: {name w/o extension}_{id}_{version}.{extension}
        # File w/o extension:  {name}_{id}_{version}
        #
        # If the entire filename is too long, {name} is truncated until it fits.
        # Append ".json" for the metadata filename. (This means that each
        # filename must leave 5 characters for this suffix.)

        name = self["name"]
        id = self["id"]
        version = self["version"]
        extension = self.get("fullFileExtension")

        # Reserve space for ".json"
        reserved_space = 5

        if self.is_folder():
            suffix = f"_{id}"
        elif self.is_workspace_doc():
            suffix = f"_{id}_{version}"
            reserved_space += WORKSPACE_EXPORT_MIME_EXTENSION_MAX_LEN
        elif extension is not None and name.endswith(extension):
            # The .endswith check is because the "fullFileExtension" field "is
            # not cleared if the new name does not contain a valid extension."
            # (https://developers.google.com/drive/api/v3/reference/files)

            suffix = f"_{id}_{version}.{extension}"
            name = name[: -(1 + len(extension))]
        else:
            suffix = f"_{id}_{version}"

        reserved_space += len(suffix)
        filename = sanitize_filename(name, reserved_space, forbidden_sub)
        return filename + suffix

    def owner_foldernames(self, forbidden_sub=None):
        # Return a list because "Only certain legacy files may have more than one owner."
        names = []
        for owner in self["owners"]:
            # "[emailAddress] may not be present in certain contexts if the
            # user has not made their email address visible to the requester."
            if "emailAddress" in owner:
                # Apparently, emails can include forbidden characters. Google
                # probably forbids this, but you can never be too sure.
                # Also, emails can't end with a period, so we don't have to worry about that.
                # (https://en.wikipedia.org/wiki/Email_address#Local-part)
                suffix = sanitize_filename(
                    owner["emailAddress"], forbidden_sub=forbidden_sub
                )
            else:
                # Each ID is a number (as a string)
                suffix = owner["permissionId"]

            # Leave space for "_", suffix, and ".json"
            name = sanitize_filename(
                owner["displayName"], 1 + len(suffix) + 5, forbidden_sub
            )
            names.append(name + "_" + suffix)

        return names


async def get_metadata_recursive(
    initial_ids,
    aiogoogle,
    drive,
    fields,
    max_concurrent,
    quota,
    out_dir,
    follow_shortcuts=True,
    follow_parents=False,
):
    """Recursively fetch the metadata of a group of IDs."""

    # We need these fields for certain things to work. The API allows us to
    # have duplicate keys, so we add them for safety. It might add a bit of
    # overhead, but it's better than failing with an obscure error if those
    # fields are left out.
    if fields != "*":
        fields = "" if fields is None else fields + ","
        fields += "id,name,mimeType,owners(displayName,permissionId,emailAddress),version,fullFileExtension"
        if follow_shortcuts:
            fields += ",shortcutDetails"
        if follow_parents:
            fields += ",parents"

    # We make requests in chunks of CHUNK_SIZE. Small chunks always prioritize
    # folders, but also defeat rate limiting. Big chunks fully utilize rate
    # limiting, but don't prioritize folders. With CHUNK_SIZE = quota * 5, each
    # chunk should take about 5 seconds. This should strike a balance between
    # the two goals.
    # If requests cluster very close together, this might still break rate
    # limiting, so it would be better to pass a queue to
    # rate_limited_as_completed. But, that can't work with the current design.
    CHUNK_SIZE = quota * 5

    ids_queue = set(initial_ids)
    ids_seen = set()

    folders_queue = set()
    folders_seen = set()
    # For folders that require multiple requests, we store their IDs and next
    # page tokens.
    folders_continue = []

    # items = defaultdict(Item)
    # Not UTC or ISO 8601, but it's readable and filename-safe
    metadata_db = (
        os.path.join(
            out_dir, "drive_temp_" + datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        )
        + ".db"
    )
    metadata_conn = sqlite3.connect(metadata_db)
    metadata_c = metadata_conn.cursor()
    metadata_c.execute(
        """
        CREATE TABLE metadata(
          id TEXT PRIMARY KEY NOT NULL,
          metadata TEXT NOT NULL
        );
        """
    )
    metadata_c.execute(
        """
        CREATE TABLE hierarchy(
          parent_id TEXT NOT NULL,
          child_id TEXT NOY NULL,
          UNIQUE (parent_id, child_id)
        );
    """
    )
    metadata_queue = []
    hierarchy_queue = []

    err_track = ErrorTracker()

    pbar_total = len(ids_queue)
    pbar = tqdm(desc="Fetch metadata", total=pbar_total, unit="req")

    def queue_parent_folder_shortcut(res):
        """Queue parent, folder, and shortcut IDs. Returns the number of queued IDs."""
        id = res["id"]
        mime_type = res["mimeType"]
        queued = 0
        if follow_parents and "parents" in res:
            for parent in res["parents"]:
                if parent not in folders_seen and parent not in folders_queue:
                    folders_queue.add(parent)
                    queued += 1
        if (
            mime_type == "application/vnd.google-apps.folder"
            and id not in folders_seen
            and id not in folders_queue
        ):
            folders_queue.add(id)
            queued += 1
        elif follow_shortcuts and mime_type == "application/vnd.google-apps.shortcut":
            target_id = res["shortcutDetails"]["targetId"]
            if target_id not in ids_seen and target_id not in ids_queue:
                # We could check the target mime type and also put this ID in
                # the folder queue if it's a folder. But, it's simpler to put
                # everything in ids_queue.
                ids_queue.add(target_id)
                queued += 1

        return queued

    async def wrap_coro(id, coro):
        return id, await coro

    def check_queue(metadata_conn, metadata_c, metadata_queue):
        if len(metadata_queue) >= 1000:
            # Should be okay to replace b/c the metadata should be the same anyway
            # but maybe some fields are populated the more we explore, e.g. parents might include more folders
            # if we've discovered more folders
            metadata_c.executemany(
                "INSERT OR REPLACE INTO metadata VALUES (?, ?)", metadata_queue
            )
            del metadata_queue[:]
            metadata_conn.commit()

    def check_queue2(metadata_conn, metadata_c, hierarchy_queue):
        if len(hierarchy_queue) >= 1000:
            metadata_c.executemany(
                "INSERT OR IGNORE INTO hierarchy VALUES (?, ?)", hierarchy_queue
            )
            del hierarchy_queue[:]
            metadata_conn.commit()

    while folders_continue or folders_queue or ids_queue:
        # Prioritize folders: they return more metadata per request
        while folders_continue or folders_queue:
            # Prioritize folders that need to be continued over new folders
            ids = folders_continue[:CHUNK_SIZE]
            del folders_continue[:CHUNK_SIZE]

            for _ in range(min(CHUNK_SIZE - len(ids), len(folders_queue))):
                id = folders_queue.pop()
                folders_seen.add(id)
                # There's no next page token
                ids.append((id, None))

            coros = [
                wrap_coro(
                    id,
                    aiogoogle.as_user(
                        drive.files.list(
                            q=f"'{id}' in parents",
                            # The `files(...)` syntax is because the files are a nested resource
                            # https://developers.google.com/drive/api/v3/fields-parameter#fetching_the_fields_of_a_nested_resource
                            fields=f"nextPageToken,incompleteSearch,files({fields})",
                            pageToken=token,
                            pageSize=PAGE_SIZE,
                        )
                    ),
                )
                for id, token in ids
            ]

            for coro in rate_limited_as_completed(coros, max_concurrent, quota):
                res = await err_track(coro)
                if not res:
                    continue
                id, res = res

                if res["incompleteSearch"]:
                    print(f"Warning: incomplete search for folder {id}")

                next_page_token = res.get("nextPageToken")
                if next_page_token:
                    folders_continue.append((id, next_page_token))
                    pbar_total += 1

                for child in res["files"]:
                    child_id = child["id"]
                    ids_seen.add(child_id)
                    try:
                        # If this ID was in the queue, we've eliminated one
                        # request, and need to update pbar_total accordingly.
                        ids_queue.remove(child_id)
                        pbar_total -= 1
                    except KeyError:
                        # If it wasn't, we'll skip the decrement.
                        pass

                    # if items[child_id].is_child:
                    #    # If this is true, then this child has two parents. For
                    #    # consistency, we'll ignore parents other than the
                    #    # first. For more info, see:
                    #    # https://developers.google.com/drive/api/v3/ref-single-parent
                    #    print(
                    #        f"Warning: folder {id} is not the only parent of {child_id}"
                    #    )
                    #    # continue

                    # items[id].children.append(child_id)
                    hierarchy_queue.append((id, child_id))
                    # items[child_id].is_child = True
                    # items[child_id].metadata = child
                    metadata_queue.append(
                        (child_id, zlib.compress(json.dumps(child).encode()))
                    )
                    check_queue(metadata_conn, metadata_c, metadata_queue)
                    check_queue2(metadata_conn, metadata_c, hierarchy_queue)
                    pbar_total += queue_parent_folder_shortcut(child)

            pbar.total = pbar_total
            pbar.update(len(coros))

        # If we don't have any more folders, do one chunk of generic IDs.
        if ids_queue:
            coros = []
            for _ in range(min(len(ids_queue), CHUNK_SIZE)):
                id = ids_queue.pop()
                ids_seen.add(id)
                coros.append(
                    aiogoogle.as_user(drive.files.get(fileId=id, fields=fields))
                )

            for coro in rate_limited_as_completed(coros, max_concurrent, quota):
                res = await err_track(coro)
                if not res:
                    continue

                # items[res["id"]].metadata = res
                # BLAH WE STILL NEED THIS
                # items[res["id"]].metadata = None
                metadata_queue.append(
                    (res["id"], zlib.compress(json.dumps(res).encode()))
                )
                check_queue(metadata_conn, metadata_c, metadata_queue)
                pbar_total += queue_parent_folder_shortcut(res)

            pbar.total = pbar_total
            pbar.update(len(coros))

    if metadata_queue:
        metadata_c.executemany(
            "INSERT OR REPLACE INTO metadata VALUES (?, ?)", metadata_queue
        )
        metadata_queue = []
        metadata_conn.commit()

    if hierarchy_queue:
        metadata_c.executemany(
            "INSERT OR IGNORE INTO hierarchy VALUES (?, ?)", hierarchy_queue
        )
        del hierarchy_queue[:]
        metadata_conn.commit()
    metadata_conn.close()

    pbar.close()

    return err_track, metadata_db


def try_mkdir(path):
    try:
        os.mkdir(path)
    except FileExistsError:
        pass


things_to_download = []
# TODO just pass args?
async def download_and_save(
    db_name,
    out_dir,
    aiogoogle,
    drive,
    max_concurrent,
    quota,
    workspace_export_mime_types,
    indent,
    forbidden_sub=None,
):
    global things_to_download
    things_to_download = []

    metadata_conn = sqlite3.connect(db_name)
    metadata_c = metadata_conn.cursor()

    def load_metadata(id):
        m = metadata_c.execute(f"SELECT metadata FROM metadata WHERE id = '{id}'")
        return json.loads(zlib.decompress(m.fetchone()[0]))

    def load_children(id):
        c = metadata_c.execute(
            f"SELECT child_id FROM hierarchy WHERE parent_id = '{id}'"
        )
        return [e[0] for e in c.fetchall()]

    def is_child(id):
        c = metadata_c.execute(f"SELECT 1 FROM hierarchy WHERE child_id = '{id}'")
        return c.fetchone() is not None

    def get_ids():
        c = metadata_c.execute("SELECT id FROM metadata")
        return [e[0] for e in c.fetchall()]

    ids = get_ids()

    pbar = tqdm(desc="Create folders, dump metadata", total=len(ids), unit="file")

    async def create_folders_dump_metadata(path, item):
        global things_to_download
        try:
            item_path = os.path.join(path, item.filename())
            with open(item_path + ".json", "w") as f:
                json.dump(item.metadata, f, indent=indent)

            pbar.update(1)

            if item.is_folder():
                try_mkdir(item_path)
                item.children = load_children(id)
                for child_id in item.children:
                    child = Item()
                    child.metadata = load_metadata(child_id)
                    await create_folders_dump_metadata(item_path, child)
                    del child.metadata
                del item.children
            else:
                if item.is_workspace_doc():
                    for mime in WORKSPACE_EXPORT[item["mimeType"]]:
                        ext = WORKSPACE_EXPORT_MIME_EXTENSION[mime]
                        things_to_download.append(
                            aiogoogle.as_user(
                                drive.files.export(
                                    fileId=item["id"],
                                    mimeType=mime,
                                    download_file=item_path + ext,
                                    alt="media",
                                    validate=False,
                                )
                            )
                        )
                else:
                    things_to_download.append(
                        aiogoogle.as_user(
                            drive.files.get(
                                fileId=item["id"],
                                download_file=item_path,
                                alt="media",
                                validate=False,
                            )
                        )
                    )

                if len(things_to_download) > 25:
                    for coro in rate_limited_as_completed(
                        things_to_download, max_concurrent, quota
                    ):
                        res = await coro
                    things_to_download = []

        except Exception as exc:
            print(f"Failed to process item: {item=}, {path=}: {exc}")
            # raise exc

    for id in ids:
        item = Item()
        item.is_child = is_child(id)
        if not item.is_child:
            try:
                item.metadata = load_metadata(id)
                for owner_foldername in item.owner_foldernames():
                    path = os.path.join(out_dir, owner_foldername)
                    try_mkdir(path)
                    await create_folders_dump_metadata(path, item)
                del item.metadata
            except Exception as exc:
                print(f"Failed to process item: {item=}: {exc}")
                # raise exc

    if things_to_download:
        for coro in rate_limited_as_completed(
            things_to_download, max_concurrent, quota
        ):
            res = await coro
        things_to_download = []
    metadata_conn.close()
    pbar.close()


async def main(ids, aiogoogle, drive, args):
    # XXX Very hacky way to increase chunk size
    import aiogoogle.models as aiogoogle_models

    aiogoogle_models.DEFAULT_DOWNLOAD_CHUNK_SIZE = 5 * 1024 * 1024

    os.makedirs(args.output, exist_ok=True)
    err_track, db_name = await get_metadata_recursive(
        ids,
        aiogoogle,
        drive,
        args.fields,
        args.concurrent,
        args.quota,
        args.output,
        args.follow_shortcuts,
        args.follow_parents,
    )

    await download_and_save(
        db_name,
        args.output,
        aiogoogle,
        drive,
        args.concurrent,
        args.quota,
        None,
        args.indent,
        None,
    )

    return err_track
