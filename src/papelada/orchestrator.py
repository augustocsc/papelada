import asyncio
import json
import time
from pathlib import Path
from collections import defaultdict
from .extractor import Extractor
from .utils import save_json, load_json 

def load_memory(path: Path) -> dict:
    # ... (código load_memory inalterado) ...
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
    
async def run(cfg: dict, extr_schema: list, processed_pdfs: dict, memory: dict, client, memory_lock: asyncio.Lock):
    all_results_dict = {} 
    background_tasks = [] 
    total_run_start_time = time.perf_counter()
    global_mode = cfg.get("mode", "standard")
    print(f"Execution Mode: {global_mode.upper()}. Total PDFs to process: {len(extr_schema)}")

    # --- ETAPA 1, 2, 3 (Pré-Análise, Agrupamento, Categorização) ... INALTERADAS ---
    job_descriptors = []
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

    parallel_warm_jobs = []       
    parallel_cold_orphans = []    
    sequential_teacher_groups = []

    if global_mode == "standard":
        sequential_teacher_groups.append([job['schema'] for job in job_descriptors])
    else: 
        for job_id, group in grouped_jobs.items():
            is_orphan = len(group) == 1
            is_warm = group[0]['is_warm'] 
            group_schemas = [job['schema'] for job in group]
            if is_warm:
                print(f"JOB GROUP {job_id[0]} (Warm): {len(group)} jobs -> Fila Paralela Rápida")
                parallel_warm_jobs.extend(group_schemas)
            elif is_orphan:
                print(f"JOB GROUP {job_id[0]} (Cold, Órfão): {len(group)} job -> Fila Paralela de Órfãos")
                parallel_cold_orphans.extend(group_schemas)
            else:
                print(f"JOB GROUP {job_id[0]} (Cold, Grupo): {len(group)} jobs -> Fila Sequencial de Ensino")
                sequential_teacher_groups.append(group_schemas)
        if global_mode == "pro":
            sequential_teacher_groups.sort(key=lambda group: (len(group), len(group[0]['extraction_schema'])), reverse=True)


    # --- Função Auxiliar de Processamento (MODIFICADA) ---
    async def process_schema(schema, effective_mode: str):
        print(f"Processing PDF: {schema['pdf_path']} (Label: {schema['label']}, Mode: {effective_mode})")
        pdf_processing_start_time = time.perf_counter()
        
        extr_ = Extractor(cfg, schema, memory, memory_lock, client, mode=effective_mode)
        
        result, task = await extr_.extract(processed_pdfs[schema['pdf_path']]['normalized_data']) 
        
        if task:
            background_tasks.append(task) 

        print(f"Extracted Data for {schema['pdf_path']}:")
        print(json.dumps(result, indent=2, ensure_ascii=False))
        
        # MUDANÇA: Anexa o objeto de métricas (que será atualizado pela task de fundo)
        result_data = {
            "label": schema["label"], 
            "pdf_path": schema["pdf_path"], 
            "extracted_data": result,
            "metrics": extr_.metrics  # Passa a referência para o objeto de métricas
        }
        
        pdf_processing_end_time = time.perf_counter()
        print(f"Finished processing {schema['pdf_path']} in {pdf_processing_end_time - pdf_processing_start_time:.2f} seconds.")
        
        all_results_dict[schema['pdf_path']] = result_data
    
    
    # --- ETAPA 4: EXECUÇÃO (Inalterada) ---
    parallel_tasks = []
    for schema in parallel_warm_jobs:
        parallel_tasks.append(process_schema(schema, "standard"))
    for schema in parallel_cold_orphans:
        parallel_tasks.append(process_schema(schema, global_mode))

    if parallel_tasks:
        print(f"\n--- Starting {len(parallel_tasks)} PARALLEL jobs (Warm + Órfãos) ---")
        await asyncio.gather(*parallel_tasks)

    if sequential_teacher_groups:
        print(f"\n--- Starting {len(sequential_teacher_groups)} SEQUENTIAL TEACHER GROUPS ---")
        for group in sequential_teacher_groups:
            teacher_schema = group.pop(0)
            student_schemas = group
            print(f"  Processing TEACHER for job {teacher_schema['label']}...")
            await process_schema(teacher_schema, global_mode)
            if student_schemas:
                print(f"  Processing {len(student_schemas)} STUDENTS for job {teacher_schema['label']} in parallel...")
                student_tasks = [process_schema(s, global_mode) for s in student_schemas]
                await asyncio.gather(*student_tasks)

    # --- FIM DA EXECUÇÃO ---
    total_run_end_time = time.perf_counter()
    print(f"Total extraction process completed in {total_run_end_time - total_run_start_time:.2f} seconds.")

    # ETAPA 5: LIMPEZA E SALVAMENTO (Inalterada)
    if background_tasks and global_mode != "standard":
        print(f"\nWaiting for {len(background_tasks)} background regex generation tasks to complete...")
        await asyncio.gather(*background_tasks)
        print("All background tasks finished.")
        try:
            memory_file_path = cfg.get("memory_file")
            if memory_file_path:
                print(f"Saving updated memory to {memory_file_path}...")
                save_json(memory, memory_file_path)
                print("Memory saved.")
            else:
                print("Warning: 'memory_file' not specified in config. Cannot save memory.")
        except Exception as e:
            print(f"Error saving memory file: {e}")
            
    # Reconstrói a lista de resultados na ordem original do 'extr_schema'
    final_results = [all_results_dict[schema['pdf_path']] for schema in extr_schema if schema['pdf_path'] in all_results_dict]
    
    # Neste ponto, os 'metrics' em final_results estão totalmente atualizados.
    return final_results