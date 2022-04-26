import re

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

# Set of owner emails to ignore
OWNER_BLACKLIST = set(
    # "example1@example.com",
    # "example2@example.com",
)

# Blacklist owner emails using regexes
REGEX_BLACKLIST = set(
    # Blacklist all .edu and .edu.* domains, case-insensitively
    # re.compile(r"\.edu(\.[^.]+)?$", re.I),
)
