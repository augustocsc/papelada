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

def save_json(data: dict, json_path: str):
    """
    Saves a dictionary to a JSON file.
    """
    p = Path(json_path)
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        with open(p, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"Error saving JSON to {json_path}: {e}")
        raise