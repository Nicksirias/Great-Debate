"""Run with: python -m backend (from project root)."""

import os

from backend.app import app

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
