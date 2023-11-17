import re


# From the `exportFormats` field of `about.get` (https://developers.google.com/drive/api/v3/reference/about/get)
WORKSPACE_EXPORT = {
    "application/vnd.google-apps.document": [
        #"application/epub+zip",
        #"application/pdf",
        #"application/rtf",
        #"application/vnd.oasis.opendocument.text",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        #"application/zip",
        #"text/html",
        #"text/plain",
    ],
    "application/vnd.google-apps.drawing": [
        #"application/pdf",
        #"image/jpeg",
        "image/png",
        #"image/svg+xml",
    ],
    "application/vnd.google-apps.form":             ["application/zip"],
    "application/vnd.google-apps.jam":              ["application/pdf"],
    "application/vnd.google-apps.mail-layout":      ["text/plain"],
    "application/vnd.google-apps.presentation": [
        #"application/pdf",
        #"application/vnd.oasis.opendocument.presentation",
        "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        #"text/plain",
    ],
    "application/vnd.google-apps.script":           ["application/vnd.google-apps.script+json"],
    "application/vnd.google-apps.site":             ["text/plain"],
    "application/vnd.google-apps.spreadsheet": [
        #"application/pdf",
        #"application/vnd.oasis.opendocument.spreadsheet",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        #"application/x-vnd.oasis.opendocument.spreadsheet",
        #"application/zip",
        #"text/csv",
        #"text/tab-separated-values",
    ],
}

# Set of owner emails to ignore
OWNER_BLACKLIST = set([
    # "example1@example.com",
    # "example2@example.com",
])

# Blacklist owner emails using regexes
REGEX_BLACKLIST = [
    # Blacklist all .edu and .edu.* domains, case-insensitively
    # re.compile(r"\.edu(\.[^.]+)?$", re.I),
]
