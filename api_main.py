import os
import json
import asyncio
import tempfile
import shutil
import time 
from pathlib import Path
from typing import List, Dict, Any, Optional 
from dotenv import load_dotenv

from fastapi import FastAPI, UploadFile, File, HTTPException, Security, Depends
from fastapi.security import APIKeyHeader
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware  

try:
    from papelada.utils import load_json, save_json
    from papelada.pipeline import load as load_pdfs
    from papelada.orchestrator import run as run_orchestrator, load_memory
    from papelada.evaluation import evaluate_accuracy 
except ImportError:
    print("Erro: Não foi possível importar os módulos de 'papelada'...")
    exit(1)

from openai import AsyncOpenAI

load_dotenv() 

app = FastAPI(
    title="Papelada API",
    description="API para extração de dados de PDFs usando uma arquitetura de aprendizagem híbrida.",
    version="1.0.0"
)

# --- Configuração de CORS (Inalterada) ---
origins = ["*"] 
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*", "POST", "OPTIONS"],
    allow_headers=["*", "X-API-Key"],
)

app_state: Dict[str, Any] = {}
API_KEY = os.getenv("PAPELADA_API_KEY", "chave-secreta-de-teste") 
api_key_header = APIKeyHeader(name="X-API-Key")

async def get_api_key(key: str = Security(api_key_header)):
    if key == API_KEY: return key
    else: raise HTTPException(status_code=403, detail="Chave de API inválida ou ausente")

# --- Eventos de Startup e Shutdown (Inalterados) ---
@app.on_event("startup")
async def startup_event():
    print("--- A carregar recursos da API... ---")
    try:
        app_state["cfg"] = load_json("config.json")
        memory_path = Path(app_state["cfg"].get("memory_file", "data/memory.json"))
        app_state["memory"] = load_memory(memory_path)
        app_state["client"] = AsyncOpenAI() 
        app_state["lock"] = asyncio.Lock()
        
        # Garante que o diretório de resultados/relatórios existe
        Path("results").mkdir(exist_ok=True)
        
        print("Recursos carregados com sucesso.")
    except Exception as e:
        print(f"ERRO FATAL ao iniciar a API: {e}")

@app.on_event("shutdown")
async def shutdown_event():
    print("--- A guardar a memória antes de desligar... ---")
    if "memory" in app_state and "cfg" in app_state:
        memory_file_path = app_state["cfg"].get("memory_file")
        if memory_file_path:
            try:
                save_json(app_state["memory"], memory_file_path)
                print("Memória guardada com sucesso.")
            except Exception as e:
                print(f"Erro ao guardar a memória: {e}")

# --- Endpoint Principal de Extração (MODIFICADO) ---

@app.post("/extract/", 
          summary="Extrair dados e opcionalmente avaliar",
          dependencies=[Depends(get_api_key)]
)
async def extract_batch(
    extraction_schema: UploadFile = File(..., description="O ficheiro JSON que define o lote de trabalho."),
    pdf_files: List[UploadFile] = File(..., description="A lista de ficheiros PDF referenciados no schema."),
    reference_data: Optional[UploadFile] = File(None, description="Opcional: Ficheiro JSON de 'ground truth' para avaliação de acurácia.")
):
    
    if "client" not in app_state or "cfg" not in app_state:
        raise HTTPException(status_code=503, detail="Serviço indisponível (falha na inicialização).")
        
    temp_dir = None
    try:
        # --- 1. Preparar Ficheiros (Inalterado) ---
        temp_dir = tempfile.mkdtemp(prefix="papelada_api_")
        temp_dir_path = Path(temp_dir)
        pdf_path_map = {} 
        print(f"A processar {len(pdf_files)} PDFs no diretório temporário: {temp_dir}")
        for pdf in pdf_files:
            file_path = temp_dir_path / pdf.filename
            try:
                with open(file_path, "wb") as f:
                    shutil.copyfileobj(pdf.file, f)
                pdf_path_map[pdf.filename] = str(file_path)
            except Exception as e:
                print(f"Erro ao guardar o ficheiro {pdf.filename}: {e}")
            finally:
                await pdf.close()

        # --- 2. Processar Schema (Inalterado) ---
        try:
            schema_content = await extraction_schema.read()
            extr_schema_list = json.loads(schema_content)
        except json.JSONDecodeError:
            raise HTTPException(status_code=400, detail="O ficheiro extraction_schema não é um JSON válido.")
        finally:
            await extraction_schema.close()
            
        # --- 3. "Hidratar" Schemas (Inalterado) ---
        valid_schemas_to_run = []
        pdf_paths_to_load = []
        for schema_job in extr_schema_list:
            pdf_name = schema_job.get("pdf_path")
            if pdf_name in pdf_path_map:
                schema_job["pdf_path_original"] = pdf_name 
                schema_job["pdf_path"] = pdf_path_map[pdf_name] 
                valid_schemas_to_run.append(schema_job)
                pdf_paths_to_load.append(pdf_path_map[pdf_name])
            else:
                print(f"Aviso: O schema para {pdf_name} foi ignorado (PDF não enviado no lote).")
        if not valid_schemas_to_run:
            raise HTTPException(status_code=400, detail="Nenhum PDF enviado corresponde aos schemas.")

        # --- 4. Preparar para o Orquestrador (Inalterado) ---
        raw_processed_pdfs = load_pdfs(pdf_paths_to_load, app_state["cfg"])
        processed_pdfs_for_orchestrator = {}
        for filename_key, data in raw_processed_pdfs.items():
            full_path_key = str(temp_dir_path / filename_key)
            processed_pdfs_for_orchestrator[full_path_key] = data

        # --- 5. Executar o Orquestrador (Inalterado) ---
        print(f"A iniciar o orquestrador para {len(valid_schemas_to_run)} trabalhos válidos...")
        final_results = await run_orchestrator(
            cfg=app_state["cfg"],
            extr_schema=valid_schemas_to_run,
            processed_pdfs=processed_pdfs_for_orchestrator, 
            memory=app_state["memory"],
            client=app_state["client"],
            memory_lock=app_state["lock"]
        )
        
        # --- 6. Limpar Resultados (Inalterado) ---
        for result in final_results:
            if "pdf_path_original" in result:
                result["pdf_path"] = result.pop("pdf_path_original")
            if "pdf_path" in result and temp_dir in result["pdf_path"]:
                result["pdf_path"] = Path(result["pdf_path"]).name
        
        # --- 7. MUDANÇA: Avaliação Opcional ---
        
        # Prepara a resposta final
        api_response = {
            "extraction_results": final_results,
            "evaluation_report": None,
            "report_saved_to": None
        }

        if reference_data:
            print("A processar ficheiro de referência para avaliação...")
            try:
                ref_content = await reference_data.read()
                ref_json = json.loads(ref_content)
                await reference_data.close()
                
                # Chame o seu novo módulo de avaliação
                # 'final_results' já contém os dados E as métricas
                report = evaluate_accuracy(
                    predictions=final_results, 
                    ground_truth=ref_json
                )
                
                # --- MUDANÇA (Início) ---
                # Adiciona um snapshot das configurações de execução ao relatório
                # para fins de reprodutibilidade e análise.
                cfg_snapshot = {
                    "mode": app_state["cfg"].get("mode", "unknown"),
                    "llm_config": app_state["cfg"].get("llm", {}),
                    "normalization_options": app_state["cfg"].get("normalization_options", {})
                }
                report["execution_config"] = cfg_snapshot
                # --- MUDANÇA (Fim) ---

                # Gera o "novo ficheiro" no servidor, como pedido
                report_filename = f"evaluation_report_{int(time.time())}.json"
                report_path = Path("results") / report_filename
                save_json(report, str(report_path))
                
                # Anexa o relatório à resposta da API
                api_response["evaluation_report"] = report
                api_response["report_saved_to"] = str(report_path)

            except Exception as e:
                print(f"Erro ao processar o ficheiro de avaliação: {e}")
                # Não falha a requisição inteira, apenas anexa o erro
                api_response["evaluation_report"] = {"error": f"Falha ao processar ficheiro de referência: {e}"}
        
        return api_response # Retorna o novo objeto de resposta estruturado

    except Exception as e:
        print(f"Erro inesperado no endpoint /extract/: {e}")
        import traceback
        traceback.print_exc() 
        raise HTTPException(status_code=500, detail=f"Erro interno do servidor: {str(e)}")
    
    finally:
        # --- 8. Limpeza (Inalterado) ---
        if temp_dir:
            try:
                shutil.rmtree(temp_dir)
                print(f"Diretório temporário {temp_dir} limpo.")
            except Exception as e:
                print(f"Erro ao limpar o diretório temporário {temp_dir}: {e}")

# --- Ponto de entrada (Inalterado) ---
if __name__ == "__main__":
    import uvicorn
    print("A iniciar o servidor Uvicorn em http://127.0.0.1:8000")
    port = int(os.getenv("PORT", 8000))
    host = "0.0.0.0" if os.getenv("RENDER") else "127.0.0.1" 
    uvicorn.run(app, host=host, port=port)