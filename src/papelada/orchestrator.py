import asyncio
import json
import time
from pathlib import Path
from collections import defaultdict
# Imports relativos para o mesmo pacote
from .extractor import Extractor

# --- NOVO: Função load_memory (Movida e Ajustada) ---
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
    
# --- run MODIFICADO: Implementa Agrupamento por 'pro' mode ---
async def run(cfg: dict, extr_schema: list, processed_pdfs: list, memory: dict, client):
    """
    Runs the extraction process...
    """
    
    
    all_results = []
    background_tasks = [] 
    memory_lock = asyncio.Lock()
    
    total_run_start_time = time.perf_counter()

    # --- IMPLEMENTAÇÃO DO MODO "PRO" COM AGRUPAMENTO ---
    mode = cfg.get("mode", "smart")
    
    if mode == "pro":
        # 1. Agrupa os esquemas por label
        grouped_schemas = defaultdict(list)
        for schema in extr_schema:
            grouped_schemas[schema['label']].append(schema)
        
        # 2. Reordena a lista de execução para processar por grupo
        ordered_schemas = []
        for label in grouped_schemas:
            ordered_schemas.extend(grouped_schemas[label])
    else: # Modo "smart" (ou qualquer outro) mantém a ordem original
        ordered_schemas = extr_schema
    
    print(f"Execution Mode: {mode.upper()}. Total PDFs to process: {len(ordered_schemas)}")
    # --- FIM DA IMPLEMENTAÇÃO DO AGRUPAMENTO ---

    for schema in ordered_schemas:
        print(f"Processing PDF: {schema['pdf_path']} (Label: {schema['label']})")
        pdf_processing_start_time = time.perf_counter()
        
        # Passa a memória compartilhada e o lock
        extr_ = Extractor(cfg, schema, memory, memory_lock, client)
        
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