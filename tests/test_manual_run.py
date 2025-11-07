import asyncio
import time
import json
import os
from dotenv import load_dotenv

# --- 1. Importações e Configuração de Base ---
from openai import AsyncOpenAI, OpenAIError

async def main():
    # Dados de Input
    campos_a_extrair_input ={
        "nome": "Nome do profissional, normalmente no canto superior esquerdo da imagem",
        "inscricao": "Número de inscrição do profissional",
        "seccional": "Seccional do profissional",
        "subsecao": "Subseção à qual o profissional faz parte",
        "categoria": "Categoria, pode ser ADVOGADO, ADVOGADA, SUPLEMENTAR, ESTAGIARIO, ESTAGIARIA",
        "endereco_profissional": "Endereço do profissional",
        "telefone_profissional": "Telefone do profissional",
        "situacao": "Situação do profissional, normalmente no canto inferior direito."
      }
    
    clened_text_input = """JOANA D'ARC
Inscrição Seccional Subseção
101943 PR CONSELHO SECCIONAL - PARANÁ
SUPLEMENTAR
Endereço Profissional
AVENIDA PAULISTA, Nº 2300 andar Pilotis, Bela Vista
SÃO PAULO - SP
01310300
Telefone Profissional
SITUAÇÃO REGULAR """
    
    # Carregamento do config.json (necessário manter se você usa arquivos externos para prompts)
    try:
        with open("config.json", "r", encoding="utf-8") as f:
            full_config = json.load(f)
            cfg = full_config.get("llm")
    except FileNotFoundError:
        print("ERRO: 'config.json' não encontrado. Certifique-se que o arquivo existe.")
        return
    
    load_dotenv()
    openai_api_key = os.getenv("OPENAI_API_KEY")
    
    if not openai_api_key:
        print("ERRO: Variável de ambiente OPENAI_API_KEY não encontrada. Certifique-se de que está definida no seu .env ou ambiente.")
        return

    # Instancia o cliente AsyncOpenAI
    llm_client = AsyncOpenAI(api_key=openai_api_key)

    extractor = LLMExtractor(
        cfg=cfg,
        campos_a_extrair=campos_a_extrair_input,
        text_to_analyze=clened_text_input,
        client=llm_client
    )
    print("🚀 Gerando regexes com o LLM...")
    result = await extractor.extract_data_json() 
    print(json.dumps(result['prompt_used'], indent=2, ensure_ascii=False))
    if "error" in result:
        print(f"❌ ERRO AO GERAR REGEX: {result['error']}")
    else:
        duration_formatted = f"{result['duration']:.3f}"
        print(f"✅ Regexes geradas com sucesso. Tempo: {duration_formatted}s")
        
        regex_list = result["json_response"]
        
        print("\n📝 JSON de Regex Gerado:")
        print(json.dumps(regex_list, indent=2, ensure_ascii=False))
        
        print("\n" + "=" * 50)
        print("🚀 Executando extração com as regexes geradas...")
        

    print("\n" + "=" * 50)
    print("Testes OpenAI concluídos.")

if __name__ == "__main__":
    asyncio.run(main())