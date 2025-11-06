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
        return path.name, {"clean_data": cleaned, "normalized_data": normalized}

    results = {}
    max_workers = min(32, (os.cpu_count() or 1) + 4)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_process, p): p for p in paths}
        for fut in as_completed(futures):
            try:
                pdf_key, data = fut.result()
                results[pdf_key] = data
            except Exception as e:
                raise RuntimeError(f"Error processing {futures[fut]}: {e}") from e

    return results
    
def run(cfg: dict, extr_schema: list, processed_pdfs: list, memory: dict = None):
    """
    Runs the extraction process on the processed PDFs using the provided configuration and extraction schema.
    First it checks the memory to find if there are rules already processed for the given label.
    Args:
        cfg: Configuration dictionary.
        extr_schema: Extraction schema dictionary.
        processed_pdfs: List of processed PDF data dictionaries.
    """
    
    from extractor import Extractor #generate_regex_json, extract_data_with_regex, 
    for schema in extr_schema:
            print(f"Processing PDF: {schema['pdf_path']}")
            extr_ = Extractor(cfg, schema)
            result = extr_.extract(processed_pdfs[schema['pdf_path']]['normalized_data'])
            print("Extracted Data:")
            print(json.dumps(result, indent=2, ensure_ascii=False))

            return 
    def _run_extraction():
        for schema in extr_schema:
            print(f"Processing PDF: {schema['pdf_path']}")
            extr_ = Extractor(cfg, schema, processed_pdfs[schema['pdf_path']])

            # Find the processed PDF data

    

            
def main(args) -> int:
#1. Loading configuration    
    try:
        # Load main config
        cfg = load_json(args.config)
        print(cfg.get("normalization_options", cfg))

        # Load the JSON config file with extraction schema
        json_path = args.extraction_schema
        extr_schema = load_json(json_path)
        
        # Create memory json file if not exists from path in config
        memory_path = Path(cfg.get("memory_file"))
        
        try:
            memory_path.parent.mkdir(parents=True, exist_ok=True)
            if not memory_path.exists():
                memory_path.write_text(json.dumps({"processed_files": []}, indent=2), encoding="utf-8")
        except Exception as e:
            print(f"Warning: unable to create cache file {memory_path}: {e}")

    except FileNotFoundError as e:
        print(f"Error: {e}")
        return 1

# 2. Load and process PDFs
    try:
        processed_pdfs = load(args.pdf_path, cfg)
    except Exception as e:
        print(f"Error processing PDFs: {e}")
        return 1
        
        # for pdf in processed_pdfs:
    #     print(f"PDF Name: {pdf['pdf_path']}")
    #     print("Clean Data:")
    #     print(pdf['clean_data'])
    #     print("Normalized Data:")
    #     print(pdf['normalized_data'])
    #     print("-" * 40)

# 3. Now the fun begins
    run(cfg, extr_schema, processed_pdfs)
    return 0

if __name__ == "__main__":
    args = parse_argrs()
    sys.exit(main(args))
