import os
import json
import asyncio
import tempfile
import shutil
import time 
import base64
from pathlib import Path
from typing import List, Dict, Any, Optional, Callable
from dotenv import load_dotenv

from fastapi import (
    FastAPI, UploadFile, File, HTTPException, Security, Depends, Body,
    WebSocket, WebSocketDisconnect
)
from fastapi.security import APIKeyHeader
from fastapi.responses import JSONResponse, FileResponse
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
    version="2.1.0 (Fluxo Async)"
)

# --- Configuração de CORS ---
origins = ["*"] 
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*", "POST", "GET", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*", "X-API-Key"],
)

app_state: Dict[str, Any] = {
    "cfg_path": "config.json",
    "memory_path_str": "data/memory.json",
}

API_KEY = os.getenv("PAPELADA_API_KEY", "chave-secreta-de-teste") 
api_key_header = APIKeyHeader(name="X-API-Key")

async def get_api_key(key: str = Security(api_key_header)):
    if key == API_KEY: return key
    else: raise HTTPException(status_code=403, detail="Chave de API inválida ou ausente")

async def get_api_key_ws(key: str):
    if key == API_KEY: return key
    else: raise HTTPException(status_code=403, detail="Chave de API inválida ou ausente")

# --- Eventos de Startup e Shutdown ---
@app.on_event("startup")
async def startup_event():
    print("--- A carregar recursos da API... ---")
    try:
        app_state["cfg"] = load_json(app_state["cfg_path"])
        app_state["memory_path_str"] = app_state["cfg"].get("memory_file", app_state["memory_path_str"])
        
        memory_path = Path(app_state["memory_path_str"])
        app_state["memory"] = load_memory(memory_path)
        
        app_state["client"] = AsyncOpenAI() 
        app_state["lock"] = asyncio.Lock()
        
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

# --- NOVO ENDPOINT DE WEBSOCKET PARA EXTRAÇÃO AO VIVO ---

@app.websocket("/ws/extract_live/")
async def websocket_extract_live(websocket: WebSocket):
    await websocket.accept()
    
    config_data = None
    temp_dir = None
    all_extraction_results = []
    
    try:
        # 1. Esperar pela mensagem de configuração e ficheiros (Base64)
        config_data = await websocket.receive_json()
        
        # 2. Validar API Key
        api_key = config_data.get("api_key")
        if not api_key or api_key != API_KEY:
            await websocket.send_json({"type": "error", "message": "Chave de API inválida ou ausente."})
            await websocket.close(code=1008)
            return

        await websocket.send_json({"type": "status", "message": "Chave de API válida. A preparar ficheiros..."})

        # 3. Preparar ficheiros a partir de Base64
        temp_dir = tempfile.mkdtemp(prefix="papelada_ws_")
        temp_dir_path = Path(temp_dir)
        pdf_path_map = {}
        
        # Ficheiro de Schema
        schema_file = config_data.get("schema_file", {})
        schema_content_b64 = schema_file.get("content", "").split(',')[-1]
        schema_content = base64.b64decode(schema_content_b64).decode('utf-8')
        extr_schema_list = json.loads(schema_content)
        
        # Ficheiros PDF
        pdf_files = config_data.get("pdf_files", [])
        for pdf in pdf_files:
            file_name = pdf.get("name")
            file_content_b64 = pdf.get("content", "").split(',')[-1]
            file_bytes = base64.b64decode(file_content_b64)
            file_path = temp_dir_path / file_name
            with open(file_path, "wb") as f:
                f.write(file_bytes)
            pdf_path_map[file_name] = str(file_path)

        # Ficheiro de Referência (Opcional)
        ref_json = None
        ref_file = config_data.get("reference_file", {})
        if ref_file.get("content"):
            ref_content_b64 = ref_file.get("content", "").split(',')[-1]
            ref_content = base64.b64decode(ref_content_b64).decode('utf-8')
            ref_json = json.loads(ref_content)

        await websocket.send_json({"type": "status", "message": f"{len(pdf_files)} PDFs prontos. A iniciar o orquestrador..."})

        # 4. "Hidratar" Schemas (igual a antes)
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

        # 5. Preparar para o Orquestrador (igual a antes)
        current_cfg = app_state["cfg"].copy()
        current_cfg["mode"] = config_data.get("mode", "smart") # Usar o modo vindo da UI
        
        raw_processed_pdfs = load_pdfs(pdf_paths_to_load, current_cfg)
        processed_pdfs_for_orchestrator = {}
        for filename_key, data in raw_processed_pdfs.items():
            full_path_key = str(temp_dir_path / filename_key)
            processed_pdfs_for_orchestrator[full_path_key] = data

        # 6. Definir o Callback de Progresso
        
        # Função de callback segura para enviar dados pelo WebSocket
        async def progress_callback(data: dict):
            try:
                # Limpa os dados para envio (remove caminhos locais)
                if data["type"] == "progress":
                    result = data["result"]
                    if "pdf_path_original" in result:
                        result["pdf_path"] = result.pop("pdf_path_original")
                    if "pdf_path" in result and temp_dir in result["pdf_path"]:
                        result["pdf_path"] = Path(result["pdf_path"]).name
                    
                    await websocket.send_json({
                        "type": "progress",
                        "result": result
                    })
                else:
                    await websocket.send_json(data)
            except Exception as e:
                print(f"Erro ao enviar callback de progresso: {e}")
        
        # 7. Executar o Orquestrador
        async with app_state["lock"]:
            memory_snapshot = app_state["memory"].copy() # Usa uma cópia para o 'run'

        # MUDANÇA: 'run' agora retorna os resultados E as tasks de fundo
        all_extraction_results, background_tasks = await run_orchestrator(
            cfg=current_cfg,
            extr_schema=valid_schemas_to_run,
            processed_pdfs=processed_pdfs_for_orchestrator, 
            memory=memory_snapshot, # O orquestrador atualiza esta cópia
            client=app_state["client"],
            memory_lock=app_state["lock"], # O lock é usado para ATUALIZAR a memória principal
            progress_callback=progress_callback
        )
        
        # MUDANÇA: Envia a 'extração concluída' IMEDIATAMENTE
        await websocket.send_json({
            "type": "extraction_complete",
            "results": all_extraction_results # Envia todos os resultados de uma vez
        })

        # 8. Avaliação Opcional (executa agora, em segundo plano da UI)
        report = None
        report_path_str = None
        
        if ref_json:
            await websocket.send_json({"type": "status", "message": "Extração concluída. A gerar relatório de avaliação..."})
            try:
                report = evaluate_accuracy(
                    predictions=all_extraction_results, 
                    ground_truth=ref_json
                )
                
                cfg_snapshot = {
                    "mode": current_cfg.get("mode", "unknown"),
                    "llm_config": current_cfg.get("llm", {}),
                    "normalization_options": current_cfg.get("normalization_options", {})
                }
                report["execution_config"] = cfg_snapshot

                report_filename = f"evaluation_report_{int(time.time())}.json"
                report_path = Path("results") / report_filename
                report_path_str = str(report_path)
                save_json(report, report_path_str)
                
                # MUDANÇA: Envia o relatório de avaliação
                await websocket.send_json({
                    "type": "evaluation_complete",
                    "evaluation_report": report,
                    "report_saved_to": report_path_str
                })
                
            except Exception as e:
                print(f"Erro ao processar o ficheiro de avaliação: {e}")
                report = {"error": f"Falha ao processar ficheiro de referência: {e}"}
                await websocket.send_json({"type": "evaluation_complete", "evaluation_report": report})
        
        # 9. Esperar pelas tasks de aprendizado em segundo plano
        if background_tasks and current_cfg.get("mode", "standard") != "standard":
            await websocket.send_json({"type": "status", "message": f"A aguardar {len(background_tasks)} tarefas de aprendizado em segundo plano..."})
            
            await asyncio.gather(*background_tasks)
            
            await websocket.send_json({"type": "learning_complete"})
            
            # Salva a memória principal (app_state["memory"]) que foi
            # modificada pelo 'extractor' usando o 'memory_lock'
            try:
                memory_file_path = current_cfg.get("memory_file")
                if memory_file_path:
                    print(f"Saving updated memory to {memory_file_path}...")
                    # Salva a memória principal do app, que foi modificada
                    async with app_state["lock"]:
                         save_json(app_state["memory"], memory_file_path)
                    print("Memory saved.")
                else:
                    print("Warning: 'memory_file' not specified in config. Cannot save memory.")
            except Exception as e:
                print(f"Error saving memory file: {e}")

        # 10. Enviar Mensagem Final de Conclusão
        await websocket.send_json({
            "type": "all_processes_complete",
            "total_files": len(all_extraction_results)
        })

    except WebSocketDisconnect:
        print("Cliente desconectado.")
    except Exception as e:
        print(f"Erro inesperado no WebSocket: {e}")
        import traceback
        traceback.print_exc()
        try:
            await websocket.send_json({"type": "error", "message": f"Erro interno do servidor: {str(e)}"})
        except:
            pass
    
    finally:
        # 11. Limpeza
        if temp_dir:
            try:
                shutil.rmtree(temp_dir)
                print(f"Diretório temporário {temp_dir} limpo.")
            except Exception as e:
                print(f"Erro ao limpar o diretório temporário {temp_dir}: {e}")
        
        if not websocket.client_state == 'DISCONNECTED':
             await websocket.close()
        print("Conexão WebSocket fechada.")


# --- ENDPOINTS DE CONFIGURAÇÃO E MEMÓRIA (Inalterados) ---

@app.get("/config/", 
         summary="Obter a configuração atual",
         dependencies=[Depends(get_api_key)])
async def get_config():
    if "cfg" not in app_state:
        raise HTTPException(status_code=503, detail="Configuração não inicializada.")
    return app_state["cfg"]

@app.put("/config/", 
         summary="Atualizar a configuração (ex: modo de execução)",
         dependencies=[Depends(get_api_key)])
async def update_config(new_config: Dict[str, Any] = Body(...)):
    if "cfg" not in app_state:
        raise HTTPException(status_code=503, detail="Configuração não inicializada.")
    
    async with app_state["lock"]:
        try:
            for key, value in new_config.items():
                app_state["cfg"][key] = value
            save_json(app_state["cfg"], app_state["cfg_path"])
            print(f"Configuração atualizada. Novo modo: {app_state['cfg'].get('mode')}")
            return {"status": "success", "new_config": app_state["cfg"]}
        except Exception as e:
            print(f"Erro ao salvar a configuração: {e}")
            raise HTTPException(status_code=500, detail=f"Erro ao salvar configuração: {e}")

@app.delete("/memory/", 
            summary="Limpar a memória de regex",
            dependencies=[Depends(get_api_key)])
async def clear_memory():
    async with app_state["lock"]:
        app_state["memory"] = {}
        try:
            save_json(app_state["memory"], app_state["memory_path_str"])
            print("Memória limpa e salva.")
            return {"status": "success", "message": "Memória limpa."}
        except Exception as e:
            print(f"Erro ao salvar a memória limpa: {e}")
            raise HTTPException(status_code=500, detail=f"Erro ao salvar memória: {e}")

@app.get("/memory/download/", 
         summary="Baixar o ficheiro de memória atual",
         dependencies=[Depends(get_api_key)])
async def download_memory():
    async with app_state["lock"]:
        save_json(app_state["memory"], app_state["memory_path_str"])
    
    return FileResponse(
        app_state["memory_path_str"],
        media_type='application/json',
        filename=Path(app_state["memory_path_str"]).name
    )

@app.post("/memory/upload/", 
          summary="Carregar um ficheiro de memória (substitui a atual)",
          dependencies=[Depends(get_api_key)])
async def upload_memory(memory_file: UploadFile = File(..., description="O ficheiro memory.json para carregar.")):
    try:
        content = await memory_file.read()
        new_memory = json.loads(content)
        
        async with app_state["lock"]:
            app_state["memory"] = new_memory
            save_json(app_state["memory"], app_state["memory_path_str"])
            print("Memória substituída por upload.")
            return {"status": "success", "message": f"Memória carregada com {len(new_memory)} labels."}
            
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="O ficheiro de memória não é um JSON válido.")
    except Exception as e:
        print(f"Erro ao carregar memória: {e}")
        raise HTTPException(status_code=500, detail=f"Erro interno ao processar ficheiro: {e}")
    finally:
        await memory_file.close()


# --- Ponto de entrada ---
if __name__ == "__main__":
    import uvicorn
    print("A iniciar o servidor Uvicorn em http://127.0.0.1:8000")
    port = int(os.getenv("PORT", 8000))
    host = "0.0.0.0" if os.getenv("RENDER") else "127.0.0.1" 
    uvicorn.run(app, host=host, port=port)