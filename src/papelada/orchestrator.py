import asyncio
import json
import time
from pathlib import Path
from collections import defaultdict, Counter 
from .extractor import Extractor
from .utils import save_json, load_json 

def load_memory(path: Path) -> dict:
    """Carrega com segurança o arquivo de memória, retornando {} em caso de falha."""
    if not path.exists():
        print(f"Memory file not found at {path}. Initializing empty memory.")
        return {}
    try:
        with open(path, 'r', encoding='utf-8') as f:
            content = f.read()
            if not content: return {}
            return json.loads(content)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Error loading memory file {path}: {e}. Initializing empty memory.")
        return {}
    
# --- Função Auxiliar de Processamento (MODIFICADA com Callback) ---
async def process_schema(
    schema: dict, 
    cfg: dict, 
    processed_pdfs: dict, 
    memory: dict, 
    client, 
    memory_lock: asyncio.Lock, 
    effective_mode: str,
    reusable_fields: set,
    background_tasks: list,
    all_results_dict: dict,
    progress_callback = None # <-- NOVO
):
    """
    Processa um único schema e chama o callback com o resultado.
    """
    print(f"Processing PDF: {schema['pdf_path']} (Label: {schema['label']}, Mode: {effective_mode})")
    
    sync_processing_start_time = time.perf_counter()
    
    extr_ = Extractor(cfg, schema, memory, memory_lock, client, mode=effective_mode)
    
    result_data_final = None 
    
    try:
        result, task = await extr_.extract(
            processed_pdfs[schema['pdf_path']]['normalized_data'], 
            reusable_fields
        )
        
        if task:
            background_tasks.append(task) 

        print(f"Data Extracted from {schema['pdf_path']}:")
        
        sync_processing_end_time = time.perf_counter()
        sync_duration = sync_processing_end_time - sync_processing_start_time

        result_data_final = {
            "label": schema["label"], 
            "pdf_path": schema["pdf_path"],
            "pdf_path_original": schema.get("pdf_path_original", schema["pdf_path"]), # <-- Incluir para callback
            "extracted_data": result,
            "metrics": extr_.metrics,
            "sync_data_time_s": round(sync_duration, 3) 
        }

    except Exception as e:
        print(f"EXTRACTOR ERROR: Falha em {schema['pdf_path']}: {e}")
        import traceback
        traceback.print_exc()
        result_data_final = {
            "label": schema["label"], 
            "pdf_path": schema["pdf_path"], 
            "pdf_path_original": schema.get("pdf_path_original", schema["pdf_path"]), # <-- Incluir para callback
            "extracted_data": extr_.extracted_data, 
            "metrics": extr_.metrics, 
            "sync_data_time_s": 0.0,
            "error": f"Extractor failed: {e}"
        }
        
    all_results_dict[schema['pdf_path']] = result_data_final
    
    # --- MUDANÇA: Chamar o Callback ---
    if progress_callback:
        try:
            await progress_callback({
                "type": "progress",
                "result": result_data_final
            })
        except Exception as e:
            print(f"Erro ao chamar o callback de progresso: {e}")
            

# --- Nova Função Auxiliar para o Modo "Pro" (MODIFICADA com Callback) ---
async def run_label_group(
    schemas_in_group: list, 
    cfg: dict, 
    processed_pdfs: dict, 
    memory: dict, 
    client, 
    memory_lock: asyncio.Lock, 
    global_mode: str, 
    reusable_fields_map: dict,
    background_tasks: list,
    all_results_dict: dict,
    progress_callback = None # <-- NOVO
):
    """
    Executa um grupo de 'label' completo sequencialmente (Regra "Pro").
    """
    
    def sort_key(schema):
        field_set = frozenset(schema['extraction_schema'].keys())
        reusable_count = len(reusable_fields_map.get(schema['label'], set()) & field_set)
        return reusable_count

    sorted_schemas = sorted(schemas_in_group, key=sort_key, reverse=True)
    
    for schema in sorted_schemas:
        await process_schema(
            schema=schema,
            cfg=cfg,
            processed_pdfs=processed_pdfs,
            memory=memory,
            client=client,
            memory_lock=memory_lock,
            effective_mode=global_mode,
            reusable_fields=reusable_fields_map.get(schema['label'], set()),
            background_tasks=background_tasks,
            all_results_dict=all_results_dict,
            progress_callback=progress_callback # <-- Passa adiante
        )

# --- run MODIFICADO (com Callback) ---
async def run(
    cfg: dict, 
    extr_schema: list, 
    processed_pdfs: dict, 
    memory: dict, 
    client, 
    memory_lock: asyncio.Lock, 
    progress_callback = None # <-- NOVO
):
    
    all_results_dict = {} 
    background_tasks = [] 
    total_run_start_time = time.perf_counter()
    global_mode = cfg.get("mode", "standard")
    print(f"Execution Mode: {global_mode.upper()}. Total PDFs to process: {len(extr_schema)}")

    # --- ETAPA 1: PRÉ-ANÁLISE (Inalterada) ---
    parallel_warm_jobs = []       
    parallel_cold_orphans = []    
    sequential_teacher_groups = []
    
    reusable_fields_map = defaultdict(set)
    field_counter = Counter()
    job_descriptors = []
    
    if global_mode != "standard":
        for schema in extr_schema:
            label = schema['label']
            for field in schema['extraction_schema'].keys():
                field_counter[(label, field)] += 1
                
        for (label, field), count in field_counter.items():
            if count > 1:
                reusable_fields_map[label].add(field)

    for schema in extr_schema:
        field_set = frozenset(schema['extraction_schema'].keys())
        job_id = (schema['label'], field_set)
        rules_for_label = memory.get(schema['label'], {})
        missing_rules_count = 0
        for field in field_set:
            if not rules_for_label.get(field): 
                missing_rules_count += 1
        
        job_descriptors.append({
            "schema": schema, "job_id": job_id,
            "is_warm": missing_rules_count == 0, "field_count": len(field_set)
        })

    grouped_jobs = defaultdict(list)
    for job in job_descriptors:
        grouped_jobs[job['job_id']].append(job)

    if global_mode == "standard":
        sequential_teacher_groups.append([job['schema'] for job in job_descriptors])
    
    else: 
        for job_id, group in grouped_jobs.items():
            is_orphan = len(group) == 1
            is_warm = group[0]['is_warm'] 
            group_schemas = [job['schema'] for job in group]
            
            label, field_set = job_id
            reusable_fields_in_group = reusable_fields_map.get(label, set()) & field_set
            
            if not reusable_fields_in_group and not is_orphan:
                print(f"JOB GROUP {job_id[0]} (Grupo sem campos reutilizáveis): {len(group)} jobs -> Fila Paralela de Órfãos")
                parallel_cold_orphans.extend(group_schemas)
            elif is_warm:
                print(f"JOB GROUP {job_id[0]} (Warm): {len(group)} jobs -> Fila Paralela Rápida")
                parallel_warm_jobs.extend(group_schemas)
            elif is_orphan:
                print(f"JOB GROUP {job_id[0]} (Cold, Órfão): {len(group)} job -> Fila Paralela de Órfãos")
                parallel_cold_orphans.extend(group_schemas)
            else:
                print(f"JOB GROUP {job_id[0]} (Cold, Grupo de Ensino): {len(group)} jobs -> Fila Sequencial/Grupo")
                sequential_teacher_groups.append(group_schemas)

    # --- ETAPA 2: EXECUÇÃO (MODIFICADA com Callback) ---
    
    parallel_tasks = []
    for schema in parallel_warm_jobs:
        parallel_tasks.append(process_schema(
            schema, cfg, processed_pdfs, memory, client, memory_lock, "standard", 
            set(), background_tasks, all_results_dict,
            progress_callback=progress_callback # <-- Passa adiante
        ))
    for schema in parallel_cold_orphans:
        parallel_tasks.append(process_schema(
            schema, cfg, processed_pdfs, memory, client, memory_lock, global_mode, 
            reusable_fields_map.get(schema['label'], set()), 
            background_tasks, all_results_dict,
            progress_callback=progress_callback # <-- Passa adiante
        ))

    if parallel_tasks:
        print(f"\n--- Starting {len(parallel_tasks)} PARALLEL jobs (Warm + Órfãos) ---")
        await asyncio.gather(*parallel_tasks)

    if sequential_teacher_groups:
        if global_mode == "pro":
            sequential_teacher_groups.sort(key=lambda group: (
                len(group), 
                len(group[0]['extraction_schema'])
            ), reverse=True)
            
            print(f"\n--- Starting {len(sequential_teacher_groups)} SEQUENTIAL TEACHER GROUPS (Modo PRO: Paralelo por Label) ---")
            
            group_tasks = []
            for group in sequential_teacher_groups:
                group_tasks.append(run_label_group(
                    group, cfg, processed_pdfs, memory, client, memory_lock, global_mode,
                    reusable_fields_map, background_tasks, all_results_dict,
                    progress_callback=progress_callback # <-- Passa adiante
                ))
            
            await asyncio.gather(*group_tasks)
            
        else:
            print(f"\n--- Starting {len(sequential_teacher_groups)} JOB GROUPS (Modo {global_mode.upper()}: Sequencial) ---")
            
            all_sequential_jobs = [schema for group in sequential_teacher_groups for schema in group]
            
            if global_mode == "smart":
                 def smart_sort_key(schema):
                    field_set = frozenset(schema['extraction_schema'].keys())
                    reusable_count = len(reusable_fields_map.get(schema['label'], set()) & field_set)
                    return reusable_count
                 all_sequential_jobs.sort(key=smart_sort_key, reverse=True)

            for schema in all_sequential_jobs:
                await process_schema(
                    schema, cfg, processed_pdfs, memory, client, memory_lock, global_mode,
                    reusable_fields_map.get(schema['label'], set()), 
                    background_tasks, all_results_dict,
                    progress_callback=progress_callback # <-- Passa adiante
                )

    # --- FIM DA EXECUÇÃO ---
    total_run_end_time = time.perf_counter()
    print(f"Total *orchestration* (sync part) completed in {total_run_end_time - total_run_start_time:.2f} seconds.")

    # --- ETAPA 3: MUDANÇA - RETORNAR TAREFAS DE FUNDO ---
    
    final_results = [all_results_dict[schema['pdf_path']] for schema in extr_schema if schema['pdf_path'] in all_results_dict]
    
    # Retorna os resultados e as tarefas de aprendizado para a API
    return final_results, background_tasks