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
from concurrent.futures import ProcessPoolExecutor, as_completed
import time
import asyncio
from datetime import datetime

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


def _process(path: Path, cfg_dict: dict) -> tuple:
    """
    Worker function for parallel PDF processing. 
    Must be defined globally for ProcessPoolExecutor to work.
    """
    raw = extract(str(path))
    cleaned = clean(raw)
    normalized = normalize(cleaned, cfg_dict.get("normalization_options", cfg_dict))
    return path.name, {"clean_data": cleaned, "normalized_data": normalized}

def load(pdf_path: str, cfg ) -> dict:
    """
    Loads and processes a the PDF(s) file(s).
    ...
    """
    # ... (código inalterado) ...
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
    if not paths:
        raise FileNotFoundError(f"No PDF files found in: {pdf_path}")

    results = {}
    max_workers = min(32, (os.cpu_count() or 1) + 4)
    cfg_for_process = cfg.to_dict() if hasattr(cfg, 'to_dict') else dict(cfg) 

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_process, p, cfg_for_process): p for p in paths}
        
        for fut in as_completed(futures):
            try:
                pdf_key, data = fut.result()
                results[pdf_key] = data
            except Exception as e:
                raise RuntimeError(f"Error processing {futures[fut]}: {e}") from e

    return results

# --- NOVO: Função load_memory (movida do extractor.py) ---
def load_memory(path: Path) -> dict:
    """Carrega com segurança o arquivo de memória, retornando {} em caso de falha."""
    if not path.exists():
        print(f"Memory file not found at {path}. Initializing empty memory.")
        return {}
    try:
        with open(path, 'r', encoding='utf-8') as f:
            content = f.read()
            if not content:
                return {}
            memory_data = json.loads(content)
            return memory_data
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Error loading memory file {path}: {e}. Initializing empty memory.")
        return {}
    
# --- run MODIFICADO ---
async def run(cfg: dict, extr_schema: list, processed_pdfs: list, memory: dict): # Aceita 'memory'
    """
    Runs the extraction process...
    """
    
    from extractor import Extractor
    all_results = []
    background_tasks = [] 
    
    # --- MODIFICADO: Cria o Lock aqui ---
    memory_lock = asyncio.Lock()
    # --- FIM DA MODIFICAÇÃO ---

    total_run_start_time = time.perf_counter()
    for schema in extr_schema:
        print(f"Processing PDF: {schema['pdf_path']}")
        pdf_processing_start_time = time.perf_counter()
        
        # --- MODIFICADO: Passa a memória compartilhada e o lock ---
        extr_ = Extractor(cfg, schema, memory, memory_lock)
        # --- FIM DA MODIFICAÇÃO ---
        
        result, task = await extr_.extract(processed_pdfs[schema['pdf_path']]['normalized_data']) 
        
        if task:
            background_tasks.append(task) 

        print("Extracted Data:")
        print(json.dumps(result, indent=2, ensure_ascii=False))
        all_results.append({"label": schema["label"], "pdf_path": schema["pdf_path"], "extracted_data": result})
        pdf_processing_end_time = time.perf_counter()
        print(f"Finished processing {schema['pdf_path']} in {pdf_processing_end_time - pdf_processing_start_time:.2f} seconds.\n")

    total_run_end_time = time.perf_counter()
    print(f"Total extraction process completed in {total_run_end_time - total_run_start_time:.2f} seconds.")

    if background_tasks:
        print(f"\nWaiting for {len(background_tasks)} background regex generation tasks to complete...")
        await asyncio.gather(*background_tasks)
        print("All background tasks finished.")

    return all_results

# --- main MODIFICADO ---
async def main(args) -> int:
    memory_path = None 
    try:
        cfg = load_json(args.config)
        json_path = args.extraction_schema
        extr_schema = load_json(json_path)
        
        memory_path = Path(cfg.get("memory_file"))
        
        # --- MODIFICADO: Carrega a memória ANTES de tudo ---
        memory_data = load_memory(memory_path)
        # Tenta criar o diretório pai, se necessário
        try:
            memory_path.parent.mkdir(parents=True, exist_ok=True)
            if not memory_path.exists():
                # Se o arquivo não existia, salva a memória vazia (ou o que load_memory leu)
                with open(memory_path, 'w', encoding='utf-8') as f:
                     json.dump(memory_data, f, indent=2)
        except Exception as e:
            print(f"Warning: unable to create cache file {memory_path}: {e}")
        # --- FIM DA MODIFICAÇÃO ---

        results_dir = Path("results")
        results_dir.mkdir(parents=True, exist_ok=True)
        output_file_path = results_dir / cfg.get("output_filename", "output.json")

    except FileNotFoundError as e:
        print(f"Error: {e}")
        return 1

    try:
        processed_pdfs = load(args.pdf_path, cfg)
    except Exception as e:
        print(f"Error processing PDFs: {e}")
        return 1
    
    # --- MODIFICADO: Passa memory_data para o run ---
    all_extraction_results = await run(cfg, extr_schema, processed_pdfs, memory_data)
    # --- FIM DA MODIFICAÇÃO ---

    if all_extraction_results:
        try:
            with open(output_file_path, 'w', encoding='utf-8') as f:
                json.dump(all_extraction_results, f, indent=2, ensure_ascii=False)
                print(f"Extraction results saved to {output_file_path}")
        except Exception as e:
            print(f"Error saving results to output file {output_file_path}: {e}")

    # Não precisamos mais da limpeza de memória aqui, pois o
    # arquivo agora é persistente e gerenciado corretamente.
            
    return 0

if __name__ == "__main__":
    args = parse_argrs()
    sys.exit(asyncio.run(main(args)))