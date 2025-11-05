#!/usr/bin/env python3
"""
main.py
Main entry point for the PDF processing pipeline.

Usage:
    python main.py path/to/config.json [path/to/pdfs_folder]
"""

import sys
import json
import argparse
import os
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

# Import from our pipeline module
from pdf_pipeline import (
        load_json,
        extract,
        clean,
        normalize
)

def parse_argrs():
        parser = argparse.ArgumentParser(description="Process PDFs according to a JSON config.")
        parser.add_argument("--extraction_schema", "-e", required=True, help="Path to JSON extraction schema file")
        parser.add_argument("--pdf_path", "-p", nargs="+", default=None, help="Path to PDFs folder, a file, or one/more file paths")
        parser.add_argument("--config", "-c", default="config.json", help="Path to config JSON file (default: config.json)")
        return parser.parse_args()

def load(pdf_path: str, cfg ) -> dict:
    """
    Loads and processes a the PDF(s) file(s). Checking if the path is a file, folder or multiple files.
    Parallel processing is used to speed up the operation.

    Args:
        pdf_path: Path to the PDF file, folder or list of files.
    Returns:
        A dictionary with the PDF name, clean data, and normalized data.
    """
    # Normalize pdf_path to a list of Path objects
    if isinstance(pdf_path, (list, tuple)):
        paths = []
        for p in pdf_path:
            p = Path(p)
            if p.is_file():
                paths.append(p)
            elif p.is_dir():
                paths.extend(sorted(p.rglob("*.pdf")))
            else:
                raise ValueError(f"Invalid path: {p}")
    else:
        p = Path(pdf_path)
        if p.is_file():
            paths = [p]
        elif p.is_dir():
            paths = sorted(p.rglob("*.pdf")) 
        else:
            raise ValueError(f"Invalid path: {pdf_path}")

    if not paths:
        raise FileNotFoundError(f"No PDF files found in: {pdf_path}")

    def _process(path: Path) -> dict:
        raw = extract(str(path))
        cleaned = clean(raw)
        normalized = normalize(cleaned, cfg.get("normalization_options", cfg))
        return {"name": path.name, "clean_data": cleaned, "normalized_data": normalized}

    results = []
    max_workers = min(32, (os.cpu_count() or 1) + 4)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_process, p): p for p in paths}
        for fut in as_completed(futures):
            try:
                results.append(fut.result())
            except Exception as e:
                raise RuntimeError(f"Error processing {futures[fut]}: {e}") from e

    return results
    
def main(args) -> int:
    # Load main config
    try:
        cfg = load_json(args.config)
        print(cfg.get("normalization_options", cfg))
    except FileNotFoundError as e:
            print(f"Error: {e}")
            return 1
    
    # 1. Load the JSON config file
    json_path = args.extraction_schema
    try:
        extr_schema = load_json(json_path)
    except FileNotFoundError as e:
        print(f"Error: {e}")
        return 1
    # 2. Load and process PDFs
    try:
        processed_pdfs = load(args.pdf_path, cfg)
    except Exception as e:
        print(f"Error processing PDFs: {e}")
        return 1
    
    # 3. Output results    
    for pdf in processed_pdfs:
        print(f"PDF Name: {pdf['name']}")
        print("Clean Data:")
        print(pdf['clean_data'])
        print("Normalized Data:")
        print(pdf['normalized_data'])
        print("-" * 40)

    return 0

if __name__ == "__main__":
    args = parse_argrs()
    sys.exit(main(args))
