from collections import defaultdict
from dataclasses import dataclass, field

from tqdm import tqdm

from rate_limit import rate_limited_as_completed
from util import ErrorTracker

# Number of results to return per folder contents request (`files.list`). Must
# be between 1 and 1000, inclusive. I assume that the biggest page size means
# the fewest requests and so the fastest speed.
PAGE_SIZE = 1000


@dataclass
class Item:
    """The result of fetching a file or folder ID."""

    metadata: dict = None
    is_child: bool = False
    children: list = field(default_factory=list)


async def get_metadata_recursive(
    initial_ids,
    aiogoogle,
    drive,
    fields,
    max_concurrent,
    quota,
    follow_shortcuts=True,
):
    """Recursively fetch the metadata of a group of IDs."""

    # We need these fields for certain things to work. The API allows us to
    # have duplicate keys, so we add them for safety. It might add a bit of
    # overhead, but it's better than failing with an obscure error if those
    # fields are left out.
    if fields is None:
        fields = "id,mimeType,exportLinks"
    else:
        fields += ",id,mimeType,exportLinks"
    if follow_shortcuts:
        fields += ",shortcutDetails"

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

    items = defaultdict(Item)
    err_track = ErrorTracker()

    pbar_total = len(ids_queue)
    pbar = tqdm(desc="Fetch metadata", total=pbar_total, unit="req")

    def queue_folders_shortcuts(res):
        """Queue folder and shortcut IDs. Returns the number of queued IDs."""
        id = res["id"]
        mime_type = res["mimeType"]
        if (
            mime_type == "application/vnd.google-apps.folder"
            and id not in folders_seen
            and id not in folders_queue
        ):
            folders_queue.add(id)
            return 1
        elif follow_shortcuts and mime_type == "application/vnd.google-apps.shortcut":
            target_id = res["shortcutDetails"]["targetId"]
            if target_id not in ids_seen and target_id not in ids_queue:
                # We could check the target mime type and also put this ID in
                # the folder queue if it's a folder. But, it's simpler to put
                # everything in ids_queue.
                ids_queue.add(target_id)
                return 1

        return 0

    async def wrap_coro(id, coro):
        return id, await coro

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

                    if items[child_id].is_child:
                        # If this is true, then this child has two parents. For
                        # consistency, we'll ignore parents other than the
                        # first. For more info, see:
                        # https://developers.google.com/drive/api/v3/ref-single-parent
                        print(
                            f"Warning: folder {id} is not the only parent of {child_id}"
                        )
                        continue

                    items[id].children.append(child_id)
                    items[child_id].is_child = True
                    items[child_id].metadata = child
                    pbar_total += queue_folders_shortcuts(child)

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

                items[res["id"]].metadata = res
                pbar_total += queue_folders_shortcuts(res)

            pbar.total = pbar_total
            pbar.update(len(coros))

    pbar.close()

    return items, err_track
