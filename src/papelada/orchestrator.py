import asyncio
import json
import time
from pathlib import Path
from collections import defaultdict, Counter 
from .extractor import Extractor
from .utils import save_json, load_json 
from typing import Callable, Awaitable, Any, Optional, Dict # Importações para o Callback

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
    progress_callback: Optional[Callable[[Dict[str, Any]], Awaitable[None]]] = None # <-- NOVO
):
    """
    Processa um único schema e chama o callback com o resultado.
    """
    print(f"Processing PDF: {schema['pdf_path']} (Label: {schema['label']}, Mode: {effective_mode})")
    
    sync_processing_start_time = time.perf_counter()
    
    # Se o cliente LLM for None (ex: modo standard sem chave), e o modo
    # não for standard, força o modo standard para evitar falhas.
    if client is None and effective_mode != "standard":
        print(f"Aviso: Nenhuma chave LLM fornecida. Forçando 'standard' (apenas memória) para este item.")
        effective_mode = "standard"
        
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
    progress_callback: Optional[Callable[[Dict[str, Any]], Awaitable[None]]] = None # <-- NOVO
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
    progress_callback: Optional[Callable[[Dict[str, Any]], Awaitable[None]]] = None # <-- NOVO
):
    
    all_results_dict = {} 
    background_tasks = [] 
    total_run_start_time = time.perf_counter()
    global_mode = cfg.get("mode", "standard")
    print(f"Execution Mode: {global_mode.upper()}. Total PDFs to process: {len(extr_schema)}")

    # --- ETAPA 1: PRÉ-ANÁLISE (Definição de Trabalhos e Campos Reutilizáveis) ---
    
    parallel_warm_jobs = []       # Fila 1: "Warm" (Regras existem)
    parallel_cold_orphans = []    # Fila 2: "Cold" e "Órfãos"
    sequential_teacher_groups = []# Fila 3: "Cold" e "Grupos"
    
    # Mapa de campos reutilizáveis (para modo "Smart" e "Pro")
    reusable_fields_map = defaultdict(set)
    field_counter = Counter()

    job_descriptors = []
    
    # 1A. Contagem de campos
    if global_mode != "standard":
        for schema in extr_schema:
            # Conta a frequência de cada campo *dentro* do seu label
            label = schema['label']
            for field in schema['extraction_schema'].keys():
                field_counter[(label, field)] += 1
                
        # Define "reutilizável" como qualquer campo que aparece mais de uma vez
        for (label, field), count in field_counter.items():
            if count > 1:
                reusable_fields_map[label].add(field)

    # 1B. Agrupamento e Categorização
    for schema in extr_schema:
        
        # --- MUDANÇA CRÍTICA: Agrupar apenas por 'label' para o Modo Smart ---
        # A granularidade do 'field_set' estava a criar apenas órfãos.
        # O comportamento "Smart" esperado é agrupar por 'label'.
        if global_mode == "smart":
            job_id = schema['label']
        else:
            # O modo "Pro" (e "Standard") pode ser mais granular
            field_set = frozenset(schema['extraction_schema'].keys())
            job_id = (schema['label'], field_set) 
        # --- FIM DA MUDANÇA ---
            
        rules_for_label = memory.get(schema['label'], {})
        missing_rules_count = 0
        for field in schema['extraction_schema'].keys():
            if not rules_for_label.get(field): 
                missing_rules_count += 1
        
        job_descriptors.append({
            "schema": schema, "job_id": job_id,
            "is_warm": missing_rules_count == 0, "field_count": len(schema['extraction_schema'])
        })

    grouped_jobs = defaultdict(list)
    for job in job_descriptors:
        grouped_jobs[job['job_id']].append(job)

    if global_mode == "standard":
        # Modo Standard: Tudo é sequencial, sem aprendizado
        # (O 'process_schema' usará effective_mode="standard")
        print("Modo 'Standard' selecionado. Todos os trabalhos serão executados sequencialmente sem aprendizado.")
        sequential_teacher_groups.append([job['schema'] for job in job_descriptors])
    
    else: # Modo Smart ou Pro
        for job_id, group in grouped_jobs.items():
            is_orphan = len(group) == 1
            is_warm = group[0]['is_warm'] 
            group_schemas = [job['schema'] for job in group]
            
            # (A lógica 'reusable_fields_in_group' foi removida pois o 'job_id' agora é diferente para 'smart')
            
            if is_warm:
                print(f"JOB GROUP {job_id} (Warm): {len(group)} jobs -> Fila Paralela Rápida")
                parallel_warm_jobs.extend(group_schemas)
            elif is_orphan:
                print(f"JOB GROUP {job_id} (Cold, Órfão): {len(group)} job -> Fila Paralela de Órfãos")
                parallel_cold_orphans.extend(group_schemas)
            else:
                # Se não for 'warm' e não for 'orphan', é um grupo de ensino
                print(f"JOB GROUP {job_id} (Cold, Grupo de Ensino): {len(group)} jobs -> Fila Sequencial/Grupo")
                sequential_teacher_groups.append(group_schemas)

    # --- ETAPA 2: EXECUÇÃO (Modo "Pro" vs "Standard/Smart") ---
    
    # Jobs Paralelos (Warm + Órfãos) são executados primeiro em todos os modos
    parallel_tasks = []
    for schema in parallel_warm_jobs:
        parallel_tasks.append(process_schema(
            schema, cfg, processed_pdfs, memory, client, memory_lock, "standard", # Modo "standard" pois é 'warm'
            set(), background_tasks, all_results_dict,
            progress_callback=progress_callback 
        ))
    for schema in parallel_cold_orphans:
        parallel_tasks.append(process_schema(
            schema, cfg, processed_pdfs, memory, client, memory_lock, global_mode, 
            reusable_fields_map.get(schema['label'], set()), 
            background_tasks, all_results_dict,
            progress_callback=progress_callback 
        ))

    if parallel_tasks:
        print(f"\n--- Starting {len(parallel_tasks)} PARALLEL jobs (Warm + Órfãos) ---")
        await asyncio.gather(*parallel_tasks)

    # Jobs Sequenciais (Grupos de Ensino)
    if sequential_teacher_groups:
        if global_mode == "pro":
            # --- Modo "Pro" ---
            # Ordena os grupos (mais alunos, mais campos)
            sequential_teacher_groups.sort(key=lambda group: (
                len(group), 
                len(group[0]['extraction_schema'])
            ), reverse=True)
            
            print(f"\n--- Starting {len(sequential_teacher_groups)} SEQUENTIAL TEACHER GROUPS (Modo PRO: Paralelo por Label) ---")
            
            # Cria uma "task" de asyncio para cada grupo de label
            group_tasks = []
            for group in sequential_teacher_groups:
                group_tasks.append(run_label_group(
                    group, cfg, processed_pdfs, memory, client, memory_lock, global_mode,
                    reusable_fields_map, background_tasks, all_results_dict,
                    progress_callback=progress_callback
                ))
            
            # Executa os grupos de label em paralelo entre si
            await asyncio.gather(*group_tasks)
            
        else:
            # --- Modo "Standard" ou "Smart" ---
            print(f"\n--- Starting {len(sequential_teacher_groups)} JOB GROUPS (Modo {global_mode.upper()}: Sequencial) ---")
            
            # Achata a lista de grupos e executa ficheiro por ficheiro, sequencialmente
            all_sequential_jobs = [schema for group in sequential_teacher_groups for schema in group]
            
            # (A ordenação "Smart" acontece aqui, dentro do loop)
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
                    progress_callback=progress_callback
                )

    # --- FIM DA EXECUÇÃO ---
    total_run_end_time = time.perf_counter()
    print(f"Total *orchestration* (sync part) completed in {total_run_end_time - total_run_start_time:.2f} seconds.")
            
    final_results = [all_results_dict[schema['pdf_path']] for schema in extr_schema if schema['pdf_path'] in all_results_dict]
    
    # Retorna os resultados e as tarefas de fundo (o 'api_main' vai dar 'await' nelas)
    return final_results, background_tasks