from dataclasses import dataclass, field
from datetime import datetime
import json
import logging
import os
import sqlite3
import traceback

from isal import isal_zlib
# Reading millions of JSON objects is slow, so we use orjson. But for maximum
# backwards compatibility, we only use it for deserializing. There's probably
# no harm to using it for serializing, but just in case.
import orjson
from tqdm import tqdm

from export_config import WORKSPACE_EXPORT, OWNER_BLACKLIST, REGEX_BLACKLIST
from rate_limit import rate_limited_as_completed
from util import ErrorTracker, sanitize_filename

now = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
logger_filename = f"drive_dl_errors_{now}.log"
print(f"Logging errors to {logger_filename}")
logging.basicConfig(filename=logger_filename)
logger = logging.getLogger(__name__)

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

    def __contains__(self, key):
        return key in self.metadata

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
        # File with extension: {name w/o extension}_{id}.{extension}
        # File w/o extension:  {name}_{id}
        #
        # If the entire filename is too long, {name} is truncated until it fits.
        # Append ".json" for the metadata filename. (This means that each
        # filename must leave 5 characters for this suffix.)
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
        elif extension is not None and extension != "" and name.endswith(extension):
            # The .endswith check is because the "fullFileExtension" field "is
            # not cleared if the new name does not contain a valid extension."
            # (https://developers.google.com/drive/api/v3/reference/files)

            suffix = f"_{id}.{extension}"
            name = name[: -(1 + len(extension))]
        else:
            suffix = f"_{id}"

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


def restore_queues(original_ids, db_name):
    # we want to redo:
    #   - folders with no children (might mean that it was in the queue, hadn't been done yet)
    #   - folders near the end of metadata (might have lost some children from batched sqlite)
    #   - folders near the end of hierarchy (same)
    #   - anything not in both metadata and hierarchy (maybe lost due to something)

    # read entire db
    # find all IDs which are folders
    # read hierarchy
    # find all folder IDs with no children
    #
    # look at N last metadata
    # pull folder ids
    # look at N last hierarchy
    # pull folder ids
    #
    # take in items
    # get rid of any in intersection(metadata, hierarchy parent/child)
    # add in folders with no children, folder ids
    #
    # this forms the queue
    # also populate seen

    # SETUP DB
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    BATCH_SIZE = 25000

    # GET ID COUNT TOTAL
    cur.execute("SELECT COUNT(*) FROM metadata")
    id_total = cur.fetchone()[0]

    # READ ENTIRE DB TO GET IDS AND FOLDER IDS
    folder_ids = set()
    non_folder_ids = set()
    pbar = tqdm(total=id_total, desc="Load all IDs from DB", unit="id")
    for offset in range(0, id_total, BATCH_SIZE):
        cur.execute(
            f"SELECT id, metadata FROM metadata LIMIT {BATCH_SIZE} OFFSET {offset}"
        )
        for id, metadata in cur.fetchall():
            metadata = orjson.loads(isal_zlib.decompress(metadata))
            if metadata["mimeType"] == "application/vnd.google-apps.folder":
                folder_ids.add(id)
            else:
                non_folder_ids.add(id)
            pbar.update(1)
    pbar.close()

    # READ HIERARCHY TO FIND FOLDERS WITH NO CHILDREN
    print("Finding folders with no children")
    cur.execute("SELECT DISTINCT(parent_id) FROM hierarchy")
    have_children = set(e[0] for e in cur.fetchall())

    # READ HIERARCHY TO DOUBLE CHECK WHICH IDS WEVE ACTUALLY SEEN
    cur.execute("SELECT DISTINCT(child_id) FROM hierarchy")
    actual_children_hierarchy = set(e[0] for e in cur.fetchall())

    # GET LAST N
    N = 1000 * 2  # Should be safe enough I think
    BACKUP = 50  # Get at least this many folders

    # GET LAST N METADATA
    # NO WE DONT DO IT BEACUSE IDS ARE RANDOM
    # we dump in a set so there's no guarantee children are processed in order from parents
    # print(f"Getting last {N} metadata")
    # cur.execute(
    #    f"SELECT DISTINCT id FROM (SELECT id FROM metadata ORDER BY rowid DESC LIMIT {N})"
    # )
    # last_n_metadata_id = set(e[0] for e in cur.fetchall())
    # GET LAST N HIERARCHY
    print(f"Getting parent folders from hierarchy last {N}")
    cur.execute(
        f"SELECT DISTINCT parent_id FROM (SELECT parent_id FROM hierarchy ORDER BY rowid DESC LIMIT {N})"
    )
    last_n_parent_id = set(e[0] for e in cur.fetchall())
    print(f"    Found: {len(last_n_parent_id)}")
    # extra extra safe--do a minimum
    if len(last_n_parent_id) < BACKUP:
        print(f"    Applying backup of {BACKUP}")
        cur.execute(
            f"SELECT DISTINCT parent_id FROM hierarchy ORDER BY rowid DESC LIMIT {BACKUP}"
        )
        last_n_parent_id |= set(e[0] for e in cur.fetchall())

    conn.close()

    print("Finding final seen/queue sets")

    # FOLDERS WE NEED TO REDO = no children + N last with no children
    # also folders which have children but no metadata
    redo_folder = (folder_ids - have_children) | (have_children - folder_ids)
    # redo_folder |= last_n_metadata_id & (folder_ids | have_children)
    redo_folder |= last_n_parent_id

    # OUTPUTTING!!!!
    export_folders_queue = redo_folder
    export_folders_seen = folder_ids - export_folders_queue

    export_ids_seen = non_folder_ids
    original_and_children_non_folder = (
        set(original_ids) | actual_children_hierarchy
    ) - (export_folders_queue | export_folders_seen)
    export_ids_queue = (
        original_and_children_non_folder | non_folder_ids
    ) - export_ids_seen

    print("Total IDs:", id_total)
    print("Restored IDs:")
    print("  folders_seen :", len(export_folders_seen))
    print("  folders_queue:", len(export_folders_queue))
    print("  ids_seen     :", len(export_ids_seen))
    print("  ids_queue    :", len(export_ids_queue))

    return export_folders_queue, export_folders_seen, export_ids_queue, export_ids_seen


# For some reason, we missed a bunch of IDs--they're in hierarchy but have no metadata.
def fix_restore_queues(db_name):
    # SETUP DB
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    BATCH_SIZE = 25000

    # GET ID COUNT TOTAL
    cur.execute("SELECT COUNT(*) FROM metadata")
    id_total = cur.fetchone()[0]

    # READ ENTIRE DB TO GET IDS AND FOLDER IDS
    folder_ids = set()
    non_folder_ids = set()
    pbar = tqdm(total=id_total, desc="Load all IDs from DB", unit="id")
    for offset in range(0, id_total, BATCH_SIZE):
        cur.execute(
            f"SELECT id, metadata FROM metadata LIMIT {BATCH_SIZE} OFFSET {offset}"
        )
        for id, metadata in cur.fetchall():
            metadata = orjson.loads(isal_zlib.decompress(metadata))
            if metadata["mimeType"] == "application/vnd.google-apps.folder":
                folder_ids.add(id)
            else:
                non_folder_ids.add(id)
            pbar.update(1)
    pbar.close()

    # GET MISSING PARENTS
    cur.execute(
        """SELECT DISTINCT parent_id FROM hierarchy WHERE NOT EXISTS (
             SELECT id FROM metadata WHERE id = parent_id)"""
    )
    missing_parents = set(e[0] for e in cur.fetchall())
    print(f"Found {len(missing_parents)} missing parents")

    cur.close()
    conn.close()

    folders_queue = set()
    # We put missing parents here because we don't want to fetch their contents
    # Only their metadata
    folders_seen = folder_ids | missing_parents
    ids_queue = missing_parents
    ids_seen = non_folder_ids | folder_ids
    return folders_queue, folders_seen, ids_queue, ids_seen


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
    restore=None,
    fix_missing_parents=None,
    indent=None,
):
    """Recursively fetch the metadata of a group of IDs."""

    # We need these fields for certain things to work. The API allows us to
    # have duplicate keys, so we add them for safety. It might add a bit of
    # overhead, but it's better than failing with an obscure error if those
    # fields are left out.
    if fields != "*":
        fields = "" if fields is None else fields + ","
        fields += "id,name,mimeType,owners(displayName,permissionId,emailAddress),version,fullFileExtension,size,resourceKey"
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

    # For folders that require multiple requests, we store their IDs and next
    # page tokens.
    folders_continue = []

    if restore is not None:
        metadata_db = restore
        folders_queue, folders_seen, ids_queue, ids_seen = restore_queues(
            initial_ids, metadata_db
        )
    elif fix_missing_parents is not None:
        metadata_db = fix_missing_parents
        folders_queue, folders_seen, ids_queue, ids_seen = fix_restore_queues(
            metadata_db
        )
    else:
        ids_queue = set(initial_ids)
        ids_seen = set()
        folders_queue = set()
        folders_seen = set()

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
        CREATE TABLE IF NOT EXISTS metadata(
          id TEXT PRIMARY KEY NOT NULL,
          metadata TEXT NOT NULL
        );
        """
    )
    # XXX: I typed "NOY NULL" instead of "NOT NULL"
    metadata_c.execute(
        """
        CREATE TABLE IF NOT EXISTS hierarchy(
          parent_id TEXT NOT NULL,
          child_id TEXT NOY NULL,
          UNIQUE (parent_id, child_id)
        );
    """
    )
    metadata_queue = []
    hierarchy_queue = []

    err_track = ErrorTracker(logger, indent)

    pbar_total = len(ids_queue) + len(folders_queue)
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

                    hierarchy_queue.append((id, child_id))
                    metadata_queue.append(
                        (child_id, isal_zlib.compress(json.dumps(child).encode()))
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

                metadata_queue.append(
                    (res["id"], isal_zlib.compress(json.dumps(res).encode()))
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
    err_track,
    db_name,
    out_dir,
    aiogoogle,
    drive,
    max_concurrent,
    quota,
    workspace_export_mime_types,
    indent,
    only_0B,
    forbidden_sub=None,
):
    global things_to_download
    things_to_download = []

    metadata_conn = sqlite3.connect(db_name)
    metadata_c = metadata_conn.cursor()

    now = datetime.now()
    print("Creating child index if it doesn't exist. This may take a very long time!")
    metadata_c.execute(
        "CREATE INDEX IF NOT EXISTS hierarchy_child ON hierarchy (child_id)"
    )
    print(f"Done, took: {datetime.now() - now}")

    metadata_c.execute(
        """
        CREATE TABLE IF NOT EXISTS downloaded (id TEXT PRIMARY KEY NOT NULL)
    """
    )

    def add_suceeded(ids):
        ids = [(i,) for i in ids]
        metadata_c.executemany("INSERT OR IGNORE INTO downloaded VALUES (?)", ids)
        metadata_conn.commit()

    def clear_suceeded(id):
        metadata_c.execute(f"DELETE FROM downloaded WHERE id = '{id}'")
        metadata_conn.commit()

    def is_suceeded(id):
        c = metadata_c.execute(f"SELECT 1 FROM downloaded WHERE id = '{id}' LIMIT 1")
        return c.fetchone() is not None

    def load_metadata(id):
        m = metadata_c.execute(
            f"SELECT metadata FROM metadata WHERE id = '{id}' LIMIT 1"
        )
        return orjson.loads(isal_zlib.decompress(m.fetchone()[0]))

    def load_children(id):
        c = metadata_c.execute(
            f"SELECT child_id FROM hierarchy WHERE parent_id = '{id}'"
        )
        return [e[0] for e in c.fetchall()]

    def is_child(id):
        c = metadata_c.execute(
            f"SELECT 1 FROM hierarchy WHERE child_id = '{id}' LIMIT 1"
        )
        return c.fetchone() is not None

    async def wrap_coro(id, coro):
        return id, await coro

    async def create_folders_dump_metadata(path, item, id_set):
        global things_to_download
        try:
            for owner in item["owners"]:
                if "emailAddress" in owner:
                    owner_email_address = owner["emailAddress"]
                    if owner_email_address in OWNER_BLACKLIST:
                        # If we're skipping a folder, we should also increment
                        # the progress bar by the number of children, but that
                        # would require recursively loading the metadata, which
                        # is slow. So we only add 1, which is inaccurate, but
                        # better than nothing.
                        download_pbar.update(1)
                        return
                    for regex in REGEX_BLACKLIST:
                        if regex.search(owner_email_address):
                            # Same as above.
                            download_pbar.update(1)
                            return

            item_path = os.path.join(path, item.filename())
            item_id = item["id"]
            if item_id in id_set:
                logger.warning(f"Loop detected: {item_id}, {item_path}")
                return

            if item.is_folder():
                id_set.add(item_id)
                try_mkdir(item_path)
                download_pbar.update(1)
                for child_id in item.children:
                    child = Item()
                    child.metadata = load_metadata(child_id)
                    child.children = load_children(child_id)
                    await create_folders_dump_metadata(item_path, child, id_set)
                    del child.metadata
                del item.children
                id_set.remove(item_id)
            else:
                if "resourceKey" in item:
                    resource_key = item["resourceKey"]
                    id_resource_key = f"{item_id}/{resource_key}"
                elif (
                    item["mimeType"] == "application/vnd.google-apps.shortcut"
                    and "shortcutDetails" in item
                    and "targetResourceKey" in item["shortcutDetails"]
                ):
                    # Can you even download a shortcut in the first place? Who knows
                    resource_key = item["shortcutDetails"]["targetResourceKey"]
                    id_resource_key = f"{item_id}/{resource_key}"
                else:
                    id_resource_key = None

                ignore_not_0B = only_0B and (not item_id.startswith("0B"))

                if not ignore_not_0B:
                    did_succeed = is_suceeded(item_id)

                if ignore_not_0B:
                    download_pbar.update(1)
                elif item.is_workspace_doc():
                    mimes_to_export = WORKSPACE_EXPORT[item["mimeType"]]
                    download_pbar.total += len(mimes_to_export) - 1

                    for mime in mimes_to_export:
                        ext = WORKSPACE_EXPORT_MIME_EXTENSION[mime]
                        # XXX: We only track 1 success for each ID, so if some
                        # export succeeded but others failed, the failed ones
                        # won't be retried
                        if (not os.path.exists(item_path + ext)) or (not did_succeed):
                            # XXX: Clears all of them which honestly doesn't do
                            # much from the problem above. If at least 1
                            # succeeds, they'll all "succeed"
                            if did_succeed:
                                clear_suceeded(item_id)
                                did_succeed = False
                            things_to_download.append(
                                wrap_coro(
                                    item_id,
                                    aiogoogle.as_user(
                                        drive.files.export(
                                            fileId=item_id,
                                            mimeType=mime,
                                            download_file=item_path + ext,
                                            id_resource_key=id_resource_key,
                                            alt="media",
                                            validate=False,
                                        )
                                    ),
                                )
                            )
                        else:
                            download_pbar.update(1)
                else:
                    # We don't check the size because the file could have
                    # changed after the initial metadata fetch
                    if (not os.path.exists(item_path)) or (not did_succeed):
                        if did_succeed:
                            clear_suceeded(item_id)
                        things_to_download.append(
                            wrap_coro(
                                item_id,
                                aiogoogle.as_user(
                                    drive.files.get(
                                        fileId=item_id,
                                        download_file=item_path,
                                        download_file_size=int(item["size"])
                                        if "size" in item
                                        else None,
                                        id_resource_key=id_resource_key,
                                        alt="media",
                                        validate=False,
                                    )
                                ),
                            )
                        )
                    else:
                        download_pbar.update(1)

            # Assume that we will never fail halfway through this operation
            if not os.path.exists(item_path + ".json"):
                with open(item_path + ".json", "w") as f:
                    json.dump(item.metadata, f, indent=indent)

            # we need to queue up a ton at once to minimize the effect of a giant file blocking everything else
            if len(things_to_download) > max_concurrent * 100:
                suceeeded = []
                for coro in rate_limited_as_completed(
                    things_to_download, max_concurrent, quota
                ):
                    res = await err_track(coro)
                    if isinstance(res, tuple):
                        suceeeded.append(res[0])
                    download_pbar.update(1)
                things_to_download = []
                if suceeeded:
                    add_suceeded(suceeeded)

        except Exception as exc:
            logger.error(f"Failed to process item: {item=}, {path=}: {exc}")
            logger.error(traceback.format_exc())

    # GET IDS INCREMENTALLY AND FETCH
    BATCH_SIZE = 50000
    metadata_c.execute("SELECT COUNT(*) FROM metadata")
    id_total = metadata_c.fetchone()[0]
    id_pbar = tqdm(total=id_total, desc="Load all IDs from DB", unit="id")
    download_pbar = tqdm(
        total=id_total,
        desc="Create folders, dump metadata, download files",
        unit="item",
    )

    for offset in range(0, id_total, BATCH_SIZE):
        metadata_c.execute(
            f"SELECT id FROM metadata LIMIT {BATCH_SIZE} OFFSET {offset}"
        )
        ids = list(e[0] for e in metadata_c.fetchall())
        id_pbar.update(len(ids))

        # ACTUALLY FETCH
        for id in ids:
            item = Item()
            item.is_child = is_child(id)
            item.children = load_children(id)
            if not item.is_child:
                try:
                    item.metadata = load_metadata(id)
                    for owner_foldername in item.owner_foldernames():
                        path = os.path.join(out_dir, owner_foldername)
                        try_mkdir(path)
                        await create_folders_dump_metadata(path, item, set())
                    del item.metadata
                except Exception as exc:
                    logger.error(f"Failed to process item: {item=}: {exc}")
                    logger.error(traceback.format_exc())

    id_pbar.close()

    if things_to_download:
        suceeeded = []
        for coro in rate_limited_as_completed(
            things_to_download, max_concurrent, quota
        ):
            res = await err_track(coro)
            if isinstance(res, tuple):
                suceeeded.append(res[0])
            download_pbar.update(1)
        if suceeeded:
            add_suceeded(suceeeded)
        things_to_download = []
    metadata_conn.close()
    download_pbar.close()


async def main(ids, aiogoogle, drive, args):
    # XXX Very hacky way to increase chunk size
    import aiogoogle.models as aiogoogle_models

    aiogoogle_models.DEFAULT_DOWNLOAD_CHUNK_SIZE = 10 * 1024 * 1024

    os.makedirs(args.output, exist_ok=True)

    if args.restore_download is None:
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
            args.restore,
            args.fix_missing_parents,
            args.indent,
        )

        await download_and_save(
            err_track,
            db_name,
            args.output,
            aiogoogle,
            drive,
            args.download_concurrent,
            args.quota,
            None,
            args.indent,
            args.only_0B,
            None,
        )
    else:
        err_track = ErrorTracker(logger, args.indent)
        await download_and_save(
            err_track,
            args.restore_download,
            args.output,
            aiogoogle,
            drive,
            args.download_concurrent,
            args.quota,
            None,
            args.indent,
            args.only_0B,
            None,
        )

    return err_track
