import os
import json
import asyncio
import tempfile
import shutil
import time 
import base64
from pathlib import Path
from typing import List, Dict, Any, Optional 
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
    version="2.1.5 (Memory Fix)"
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
        
        # O cliente OpenAI será criado DENTRO do websocket_extract_live para usar a chave passada
        # Adiciona a fábrica de clientes ao estado
        app_state["client_factory"] = lambda key: AsyncOpenAI(api_key=key)
        
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
    print("\nDEBUG: WebSocket Conectado. Aguardando mensagem de configuração...")
    
    config_data = None
    temp_dir = None
    all_extraction_results = []
    
    try:
        # 1. Esperar pela mensagem de configuração e ficheiros (Base64)
        config_data = await websocket.receive_json()
        print("DEBUG: Mensagem de configuração recebida.")
        
        # 2. Validar API Key
        papelada_api_key = config_data.get("papelada_api_key")
        if not papelada_api_key or papelada_api_key != API_KEY:
            print(f"DEBUG: Falha na autenticação da Papelada API Key. Chave recebida: {papelada_api_key}")
            await websocket.send_json({"type": "error", "message": "Chave de API Papelada inválida ou ausente."})
            await websocket.close(code=1008)
            return

        # --- Lógica de verificação da chave OpenAI ---
        # 1. Pega a chave do frontend (pode ser "" ou None)
        openai_api_key = config_data.get("openai_api_key")
        # 2. Se não houver chave do frontend (é "" ou None), TENTA pegar do .env
        if not openai_api_key:
            openai_api_key = os.getenv("OPENAI_API_KEY")

        current_mode = config_data.get("mode", "smart")
        llm_client = None
        
        print(f"DEBUG: Chave Papelada OK. Chave OpenAI (após fallback): {'*' * len(openai_api_key) if openai_api_key else 'None'}. Modo: {current_mode}")

        # 3. AGORA, verifica se (após todas as tentativas) a chave ainda está ausente
        if openai_api_key:
            # Se uma chave FOI encontrada (no app ou .env), tente usá-la.
            try:
                llm_client = app_state["client_factory"](openai_api_key)
                await websocket.send_json({"type": "status", "message": "Cliente LLM (OpenAI) inicializado com sucesso."})
                print("DEBUG: Cliente LLM (OpenAI) inicializado com sucesso.")
            except Exception as e:
                # Se a chave for inválida (ex: "sk-123"), isso é um erro fatal.
                print(f"DEBUG: Falha ao inicializar cliente OpenAI. Chave inválida? Erro: {e}")
                await websocket.send_json({"type": "error", "message": f"Falha ao inicializar cliente OpenAI (Chave inválida?): {e}"})
                await websocket.close(code=1008)
                return
        else:
            # Se NENHUMA chave foi encontrada
            if current_mode != "standard":
                # Apenas envie um AVISO. O Orquestrador vai falhar se precisar do LLM.
                print("DEBUG: Nenhuma chave OpenAI encontrada, mas o modo 'smart'/'pro' foi selecionado. Enviando aviso.")
                await websocket.send_json({"type": "status", "message": "Aviso: Chave OpenAI não fornecida. A extração por LLM falhará. Apenas regras de memória funcionarão."})
            else:
                # Modo Standard, tudo bem.
                print("DEBUG: Nenhuma chave OpenAI encontrada. Modo 'standard' selecionado. OK.")
                await websocket.send_json({"type": "status", "message": "Chave OpenAI não fornecida. A executar em modo 'Standard' (apenas memória)."})
        
        # O código agora continua, mesmo que llm_client seja None.
        # --- FIM DA MUDANÇA ---


        await websocket.send_json({"type": "status", "message": "Chaves válidas. A preparar ficheiros..."})
        print("DEBUG: Lendo e decodificando arquivos...")

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
        ref_file = config_data.get("reference_file") # Pode ser None
        if ref_file and ref_file.get("content"): # Verifica se ref_file não é None
            ref_content_b64 = ref_file.get("content", "").split(',')[-1]
            ref_content = base64.b64decode(ref_content_b64).decode('utf-8')
            ref_json = json.loads(ref_content)
            print("DEBUG: Arquivo de referência (teste) carregado.")
        
        await websocket.send_json({"type": "status", "message": f"{len(pdf_files)} PDFs prontos. A iniciar o orquestrador..."})
        print("DEBUG: Arquivos prontos. Hidratando schemas...")

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
            print("DEBUG: Erro - Nenhum PDF corresponde aos schemas.")
            await websocket.send_json({"type": "error", "message": "Nenhum PDF enviado corresponde aos schemas."})
            await websocket.close(code=1008)
            return

        print("DEBUG: Schemas prontos. Carregando PDFs...")
        # 5. Preparar para o Orquestrador
        current_cfg = app_state["cfg"].copy()
        current_cfg["mode"] = config_data.get("mode", "smart")
        
        raw_processed_pdfs = load_pdfs(pdf_paths_to_load, current_cfg)
        processed_pdfs_for_orchestrator = {}
        for filename_key, data in raw_processed_pdfs.items():
            full_path_key = str(temp_dir_path / filename_key)
            processed_pdfs_for_orchestrator[full_path_key] = data

        print("DEBUG: PDFs carregados. Iniciando Orquestrador...")
        # 6. Definir o Callback de Progresso
        
        async def progress_callback(data: dict):
            """Função injetada no orquestrador para enviar atualizações."""
            if data["type"] == "progress":
                print(f"DEBUG: Orquestrador enviou progresso para {data['result'].get('pdf_path_original')}")
                # Adiciona o resultado à nossa lista
                result = data["result"]
                
                # Limpa o resultado para envio (remove dados locais)
                if "pdf_path_original" in result:
                    # Usa o nome original do PDF para a UI
                    result["pdf_path"] = result.pop("pdf_path_original") 
                
                all_extraction_results.append(result)
                
                # Envia a atualização de progresso para a UI (EXTRAÇÃO INDIVIDUAL)
                await websocket.send_json({
                    "type": "progress",
                    "result": result
                })
            elif data["type"] == "error":
                print(f"DEBUG: Orquestrador enviou erro: {data['message']}")
                await websocket.send_json(data)
        
        # 7. Executar o Orquestrador (Não espera pelas tarefas de aprendizado)
        # --- MUDANÇA CRÍTICA: Remover .copy() ---
        # Devemos passar a REFERÊNCIA para a memória, não uma cópia.
        async with app_state["lock"]:
            memory_to_use = app_state["memory"] # Passa a referência direta

        initial_results, background_tasks = await run_orchestrator(
            cfg=current_cfg,
            extr_schema=valid_schemas_to_run,
            processed_pdfs=processed_pdfs_for_orchestrator, 
            memory=memory_to_use, # Passa a referência, não o snapshot
            client=llm_client, # Pode ser None
            memory_lock=app_state["lock"],
            progress_callback=progress_callback
        )
        # --- FIM DA MUDANÇA ---
        
        print("DEBUG: Orquestrador CONCLUÍDO (Extração Síncrona). Enviando 'extraction_complete'...")
        # 8. Enviar Mensagem de Extração COMPLETA (SINAL PARA MUDAR DE TELA)
        await websocket.send_json({
            "type": "extraction_complete",
            "results": initial_results,
        })
        
        # --- PROCESSOS DE FUNDO INICIAM AQUI ---
        
        # 9. Avaliação Opcional (Execução no background, mas aguardada para envio)
        report = None
        report_path_str = None
        
        if ref_json:
            print("DEBUG: Processando avaliação (se houver)...")
            await websocket.send_json({"type": "status", "message": "A gerar relatório de avaliação..."})
            try:
                # O evaluate_accuracy usa os resultados finais (initial_results)
                report = evaluate_accuracy(
                    predictions=initial_results, 
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
                
            except Exception as e:
                print(f"Erro ao processar o ficheiro de avaliação: {e}")
                report = {"error": f"Falha ao processar ficheiro de referência: {e}"}
        
        print("DEBUG: Avaliação CONCLUÍDA. Enviando 'evaluation_complete'...")
        # Envia o relatório de avaliação para a UI (ATUALIZAÇÃO DINÂMICA)
        await websocket.send_json({
            "type": "evaluation_complete",
            "evaluation_report": report,
            "report_saved_to": report_path_str
        })
        
        # 10. Esperar e Notificar sobre o Aprendizado de Regras (LENTO)
        if background_tasks:
            print(f"DEBUG: Aguardando {len(background_tasks)} tarefas de aprendizado...")
            await websocket.send_json({"type": "status", "message": f"A aguardar {len(background_tasks)} tarefas de aprendizado de regras..."})
            await asyncio.gather(*background_tasks, return_exceptions=True)
            
            print("DEBUG: Tarefas de aprendizado CONCLUÍDAS. Enviando 'learning_complete'...")
            # Envia a notificação de aprendizado completo
            await websocket.send_json({"type": "learning_complete"})
            
        # 11. Mensagem Final
        print("DEBUG: Todos os processos concluídos. Enviando 'all_processes_complete'.")
        await websocket.send_json({"type": "all_processes_complete"})


    except WebSocketDisconnect:
        print("Cliente desconectado.")
    except Exception as e:
        print(f"Erro inesperado no WebSocket: {e}")
        import traceback
        traceback.print_exc()
        try:
            await websocket.send_json({"type": "error", "message": f"Erro interno do servidor: {str(e)}"})
        except:
            pass # A conexão pode estar morta.
    
    finally:
        # 12. Limpeza
        if temp_dir:
            try:
                shutil.rmtree(temp_dir)
                print(f"Diretório temporário {temp_dir} limpo.")
            except Exception as e:
                print(f"Erro ao limpar o diretório temporário {temp_dir}: {e}")
        
        # --- Correção da Race Condition ---
        if websocket.client_state != 'DISCONNECTED':
             try:
                 await websocket.close()
                 print("Conexão WebSocket fechada pelo servidor.")
             except RuntimeError as e:
                 # Captura a race condition e a ignora silenciosamente (ou com log limpo)
                 print(f"Info: WebSocket já fechado (provavelmente pelo cliente). A race condition foi capturada.")
        else:
            print("Info: WebSocket já estava desconectado.")


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
        # ESTA É A LINHA QUE SALVA ANTES DE BAIXAR
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