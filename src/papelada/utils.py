import json
from pathlib import Path
from typing import Any

def load_json(json_path: str) -> Any:
    """
    Loads and parses a JSON file.
    Raises FileNotFoundError if the file does not exist.
    """
    p = Path(json_path)
    if not p.is_file():
        raise FileNotFoundError(f"JSON file not found: {json_path}")
    return json.loads(p.read_text(encoding="utf-8"))