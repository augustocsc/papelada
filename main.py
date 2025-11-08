#!/usr/bin/env python3
"""
main.py
Ponto de entrada principal para o pipeline Papelada.
"""

import sys
import json
import argparse
import os
from pathlib import Path
import asyncio
from dotenv import load_dotenv # Importar dotenv aqui
from openai import AsyncOpenAI # Importar AsyncOpenAI aqui

# --- Imports do seu novo pacote 'papelada' ---
from papelada.utils import load_json
from papelada.pipeline import load as load_pdfs
from papelada.orchestrator import run as run_orchestrator, load_memory

def parse_argrs():
        parser = argparse.ArgumentParser(description="Process PDFs according to a JSON config.")
        parser.add_argument("--extraction_schema", "-e", required=True, help="Path to JSON extraction schema file")
        parser.add_argument("--pdf_path", "-p", nargs="+", default=None, help="Path to PDFs folder, a file, or one/more file paths")
        parser.add_argument("--config", "-c", default="config.json", help="Path to config JSON file (default: config.json)")
        return parser.parse_args()


async def main(args) -> int:
    memory_path = None 
    try:
        cfg = load_json(args.config)
        json_path = args.extraction_schema
        extr_schema = load_json(json_path)
        
        memory_path = Path(cfg.get("memory_file", "data/memory.json"))
        clean_memory_on_start = cfg.get("clean_memory_on_start", False)
        memory_data = load_memory(memory_path, clean_start=clean_memory_on_start)

        results_dir = Path("results")
        results_dir.mkdir(parents=True, exist_ok=True)
        output_file_path = results_dir / cfg.get("output_filename", "output.json")

    except FileNotFoundError as e:
        print(f"Error: {e}")
        return 1

    try:
        processed_pdfs = load_pdfs(args.pdf_path, cfg)
    except Exception as e:
        print(f"Error processing PDFs: {e}")
        return 1
    
    # --- Injeção de Dependência ---
    # 1. Carregue o .env
    load_dotenv()
    if not os.getenv("OPENAI_API_KEY"):
        print("⚠️ AVISO: OPENAI_API_KEY não encontrada nas variáveis de ambiente.")
        return 1
        
    # 2. Instancie o cliente AQUI
    client = AsyncOpenAI()
    
    # 3. Passe o cliente para o orquestrador
    all_extraction_results = await run_orchestrator(cfg, extr_schema, processed_pdfs, memory_data, client)

    if all_extraction_results:
        try:
            with open(output_file_path, 'w', encoding='utf-8') as f:
                json.dump(all_extraction_results, f, indent=2, ensure_ascii=False)
                print(f"Extraction results saved to {output_file_path}")
        except Exception as e:
            print(f"Error saving results to output file {output_file_path}: {e}")
            
    return 0

if __name__ == "__main__":
    args = parse_argrs()
    sys.exit(asyncio.run(main(args)))