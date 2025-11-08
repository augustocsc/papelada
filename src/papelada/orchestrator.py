import asyncio
import json
import time
from pathlib import Path
from collections import defaultdict
# Imports relativos para o mesmo pacote
from .extractor import Extractor
from .utils import save_json, load_json 

# --- Função load_memory (Inalterada) ---
def load_memory(path: Path, clean_start: bool = False) -> dict:
    """Carrega com segurança o arquivo de memória, retornando {} em caso de falha."""
    if clean_start and path.exists():
        print(f"Clean start requested. Deleting existing memory file at {path}.")
        path.unlink() # Deleta o arquivo

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
    
# --- run MODIFICADO: Implementa Lógica "Pro" Avançada ---
async def run(cfg: dict, extr_schema: list, processed_pdfs: dict, memory: dict, client, memory_lock: asyncio.Lock):
    """
    Executa o processo de extração com um agendador dinâmico de 3 filas
    baseado na assinatura de campos e no estado da memória (Warm/Cold).
    """
    
    all_results_dict = {} # Usar um dict para manter a ordem original no final
    background_tasks = [] 
    #memory_lock = asyncio.Lock()
    
    total_run_start_time = time.perf_counter()

    global_mode = cfg.get("mode", "standard")
    print(f"Execution Mode: {global_mode.upper()}. Total PDFs to process: {len(extr_schema)}")

    # --- ETAPA 1: PRÉ-ANÁLISE E DEFINIÇÃO DE TRABALHO ---
    # Aqui, inspecionamos CADA schema ANTES da execução para
    # construir uma assinatura de trabalho e verificar o status da memória.
    
    job_descriptors = []
    for schema in extr_schema:
        # 1. Cria uma "assinatura" dos campos de extração
        # Usamos frozenset para que possa ser usado como chave de dicionário
        field_set = frozenset(schema['extraction_schema'].keys())
        
        # 2. O "ID do Trabalho" é a combinação do label + assinatura de campos
        # Isso agrupa A1+A2 (mesmo label, mesmos campos) e B1+B2 (mesmo label, mesmos campos)
        # mas separa B3 (mesmo label, campos DIFERENTES)
        job_id = (schema['label'], field_set)
        
        # 3. Verifica o status "Warm/Cold" (Warm-up Check)
        rules_for_label = memory.get(schema['label'], {})
        missing_rules_count = 0
        for field in field_set:
            # Consideramos "warm" se o campo (ex: "nome") existe E tem pelo menos uma regra
            if not rules_for_label.get(field): 
                missing_rules_count += 1
                
        job_descriptors.append({
            "schema": schema,
            "job_id": job_id,
            "is_warm": missing_rules_count == 0,
            "field_count": len(field_set)
        })

    # --- ETAPA 2: AGRUPAMENTO POR "ID DO TRABALHO" ---
    # Agrupa todos os trabalhos que são identicos (ex: A1, A2, A3)
    grouped_jobs = defaultdict(list)
    for job in job_descriptors:
        grouped_jobs[job['job_id']].append(job)

    # --- ETAPA 3: CATEGORIZAÇÃO DAS 3 FILAS DE EXECUÇÃO ---
    
    parallel_warm_jobs = []       # Fila 1: "Warm" (Regras existem). Roda 100% paralelo.
    parallel_cold_orphans = []    # Fila 2: "Cold" e "Órfãos". Roda paralelo, mas aprende.
    sequential_teacher_groups = []# Fila 3: "Cold" e "Grupos". Roda 1 "professor" e N "alunos".

    # Se o modo for "standard", tratamos tudo como um grande grupo sequencial
    if global_mode == "standard":
        sequential_teacher_groups.append([job['schema'] for job in job_descriptors])
    
    else: # Modo "smart" ou "pro"
        for job_id, group in grouped_jobs.items():
            is_orphan = len(group) == 1
            is_warm = group[0]['is_warm'] # Todos no grupo têm o mesmo status warm/cold
            
            # Pega os schemas originais de volta
            group_schemas = [job['schema'] for job in group]

            if is_warm:
                # Fila 1: Regras já existem.
                # Não importa se é órfão ou grupo. Vão todos para a fila paralela rápida.
                print(f"JOB GROUP {job_id[0]} (Warm): {len(group)} jobs -> Fila Paralela Rápida")
                parallel_warm_jobs.extend(group_schemas)
            
            elif is_orphan:
                # Fila 2: Regras não existem, mas é órfão (ex: B3 ou C).
                # Vai para a fila paralela, mas vai aprender (para o *próximo* lote).
                print(f"JOB GROUP {job_id[0]} (Cold, Órfão): {len(group)} job -> Fila Paralela de Órfãos")
                parallel_cold_orphans.extend(group_schemas)
            
            else:
                # Fila 3: Regras não existem E é um grupo (ex: A1, A2, A3).
                # Este é um grupo de "Professor/Alunos".
                print(f"JOB GROUP {job_id[0]} (Cold, Grupo): {len(group)} jobs -> Fila Sequencial de Ensino")
                sequential_teacher_groups.append(group_schemas)

        if global_mode == "pro":
            # Ordena a Fila 3 para otimização máxima
            # 1. Grupos com mais "alunos" primeiro
            # 2. Grupos com mais "campos" (mais aprendizado) primeiro
            sequential_teacher_groups.sort(key=lambda group: (
                len(group), 
                len(group[0]['extraction_schema'])
            ), reverse=True)


    # --- Função Auxiliar de Processamento ---
    async def process_schema(schema, effective_mode: str):
        """Processa um único schema, usando o modo_efetivo fornecido."""
        # O 'extractor.py' não mudou e ainda aceita 'mode'
        print(f"Processing PDF: {schema['pdf_path']} (Label: {schema['label']}, Mode: {effective_mode})")
        pdf_processing_start_time = time.perf_counter()
        
        extr_ = Extractor(cfg, schema, memory, memory_lock, client, mode=effective_mode)
        
        result, task = await extr_.extract(processed_pdfs[schema['pdf_path']]['normalized_data']) 
        
        if task:
            background_tasks.append(task) 

        print(f"Extracted Data for {schema['pdf_path']}:")
        print(json.dumps(result, indent=2, ensure_ascii=False))
        
        result_data = {"label": schema["label"], "pdf_path": schema["pdf_path"], "extracted_data": result}
        
        pdf_processing_end_time = time.perf_counter()
        print(f"Finished processing {schema['pdf_path']} in {pdf_processing_end_time - pdf_processing_start_time:.2f} seconds.")
        
        all_results_dict[schema['pdf_path']] = result_data
    
    
    # --- ETAPA 4: EXECUÇÃO (em 3 estágios) ---

    # Estágio 1: Executa Fila 1 (Warm) e Fila 2 (Órfãos) em paralelo total
    parallel_tasks = []
    
    # Fila 1 (Warm) é forçada ao modo "standard" (sem aprendizado, pois já sabe)
    for schema in parallel_warm_jobs:
        parallel_tasks.append(process_schema(schema, "standard"))
        
    # Fila 2 (Órfãos) usa o global_mode (para aprender para o *futuro*)
    for schema in parallel_cold_orphans:
        parallel_tasks.append(process_schema(schema, global_mode))

    if parallel_tasks:
        print(f"\n--- Starting {len(parallel_tasks)} PARALLEL jobs (Warm + Órfãos) ---")
        await asyncio.gather(*parallel_tasks)

    # Estágio 2: Executa Fila 3 (Grupos de Ensino) de forma híbrida
    if sequential_teacher_groups:
        print(f"\n--- Starting {len(sequential_teacher_groups)} SEQUENTIAL TEACHER GROUPS ---")
        
        for group in sequential_teacher_groups:
            # Pega o primeiro como "Professor"
            teacher_schema = group.pop(0)
            student_schemas = group # O resto são "Alunos"

            # 1. Executa o Professor SOZINHO e espera ele terminar
            print(f"  Processing TEACHER for job {teacher_schema['label']}...")
            await process_schema(teacher_schema, global_mode)
            
            # 2. Executa todos os Alunos EM PARALELO
            if student_schemas:
                print(f"  Processing {len(student_schemas)} STUDENTS for job {teacher_schema['label']} in parallel...")
                student_tasks = [process_schema(s, global_mode) for s in student_schemas]
                await asyncio.gather(*student_tasks)

    # --- FIM DA EXECUÇÃO ---

    total_run_end_time = time.perf_counter()
    print(f"Total extraction process completed in {total_run_end_time - total_run_start_time:.2f} seconds.")

    # ETAPA 5: LIMPEZA E SALVAMENTO
    # (A lógica é a mesma, mas só salva se o modo não for "standard")
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

    return final_results