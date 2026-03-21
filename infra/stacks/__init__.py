from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
EXCLUDE_DIRS = [
    ".git",
    "*.pyc",
    "__pycache__",
    ".venv",
    "cdk.out",
    "node_modules",
    "infra",
    "ml",
    "processing",
    "inference",
    "ingestion",
    "docs",
    ".github",
]