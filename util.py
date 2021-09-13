from collections import defaultdict
from datetime import datetime
import json
import re

from aiogoogle.excs import HTTPError

ASCII_CONTROL = re.compile(r"[\x00-\x1f\x7f]")
FILENAME_FORBIDDEN = re.compile(r'[\\/:*?"<>|]')
LEADING_DASH_DOT = re.compile(r"_*[-.]")
TRAILING_DOTS = re.compile(r"\.+$")

# TODO https://en.wikipedia.org/wiki/Comparison_of_file_systems#Limits
MAX_BYTES = 255

FORBIDDEN_SUB = {
    "\\": "⧹",
    "/": "⧸",
    ":": "꞉",
    "*": "∗",
    "?": "？",
    '"': "″",
    "<": "﹤",
    ">": "﹥",
    "|": "￨",
}


def sanitize_filename(filename, reserved_space=0, forbidden_sub=None):
    forbidden_sub = FORBIDDEN_SUB
    filename = ASCII_CONTROL.sub("", filename)
    if forbidden_sub is None:
        # The forbidden characters are common enough that we replace them with an
        # underscore to show that a character was replaced.
        filename = FILENAME_FORBIDDEN.sub("_", filename)
    else:
        filename = FILENAME_FORBIDDEN.sub(lambda m: forbidden_sub[m[0]], filename)
    filename = filename.strip()
    # Prepend an underscore to files starting with a dot or dash--this prevents
    # the file from being hidden or being interpreted as a flag on Unix-likes.
    # We need _* to ensure that prepending an underscore doesn't conflict with
    # any existing files.
    if LEADING_DASH_DOT.match(filename):
        filename = "_" + filename
    # Limit the filename to `MAX_BYTES - reserved_space` at most. We assume UTF-8.
    max_bytes = MAX_BYTES - reserved_space
    encoded = filename.encode()
    if len(encoded) > max_bytes:
        filename = encoded[:max_bytes].decode(errors="ignore")
        # Truncation might have exposed trailing spaces, so we strip again.
        filename = filename.strip()
    return filename


class ErrorTracker:
    """Filter out and track errors from Aiogoogle coroutines."""

    def __init__(self, indent=None):
        self.errors = []
        self.counts = defaultdict(int)
        self.total = 0

        now = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        self.filename = f"drive_errors_{now}.json"
        self.indent = indent

    def print_errors(self):
        print("Error summary:")
        if self.counts:
            for code, count in self.counts.items():
                print(f"  {code}: {count}")

            with open(self.filename, "w") as f:
                json.dump(self.errors, f, indent=self.indent)
            print(f"Wrote errors to {self.filename}")
        else:
            print("  No errors.")

    async def __call__(self, coro):
        try:
            return await coro
        except HTTPError as exc:
            error = {"url": exc.req.url}
            if exc.res.json is not None and "error" in exc.res.json:
                json_error = exc.res.json["error"]
                error.update(
                    {
                        "code": json_error["code"],
                        "message": json_error["message"],
                    }
                )
                self.counts[error["code"]] += 1
                self.total += 1
                if self.total % 5000 == 0:
                    self.print_errors()

            self.errors.append(error)
        except Exception as exc:
            print(exc)
