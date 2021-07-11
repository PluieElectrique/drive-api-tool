from collections import defaultdict


class ErrorTracker:
    """Filter out and track errors from Aiogoogle coroutines."""

    def __init__(self):
        self.errors = []
        self.counts = defaultdict(int)

    async def __call__(self, coro):
        try:
            return await coro
        except Exception as exc:
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

            self.errors.append(error)
