"""Application entrypoint for running the API locally."""

import os
import uvicorn

try:
    from feedback_api.api import create_app
except ModuleNotFoundError:
    from .feedback_api.api import create_app

app = create_app()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=port)
