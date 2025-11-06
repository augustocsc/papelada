import asyncio
import time
import json
import re
import os
from dotenv import load_dotenv

# --- 1. Importações e Configuração de Base ---
from openai import AsyncOpenAI, OpenAIError

# Carrega variáveis de ambiente (.env)
load_dotenv()

# Verifica se a chave da OpenAI está disponível
if not os.getenv("OPENAI_API_KEY"):
    print("⚠️ AVISO: OPENAI_API_KEY não encontrada nas variáveis de ambiente.")

class LLMExtractor:
    """
    Encapsula a lógica para geração de Regex estruturada via LLM (OpenAI Nativo)
    e a subsequente aplicação das regexes para extração de dados.
    """

    def __init__(self, cfg: dict, campos_a_extrair: list, text_to_analyze: str):
        """
        Inicializa o extrator, carregando o schema e o prompt.
        """
        self.model_name = cfg['model_name']
        # Inicializa o cliente assíncrono da OpenAI
        # Ele pegará automaticamente a OPENAI_API_KEY do ambiente.
        self.client = AsyncOpenAI()
        
        print(f"Usando o modelo (OpenAI): {self.model_name}")
        self.campos_a_extrair = campos_a_extrair
        self.text_to_analyze = text_to_analyze
        self._validate_campos()
        
        # Carrega a configuração externa
        # Nota: A OpenAI nativa não usa o schema diretamente no response_format 
        # da mesma forma que o LiteLLM abstrai, a menos que use "Structured Outputs" (json_schema).
        # Para manter simples e compatível com a maioria dos modelos atuais, 
        # usaremos o modo JSON padrão e confiaremos que o seu prompt já instrui a estrutura.
        self.response_schema_dict = cfg['output_schema']
        
        with open(cfg['prompt_file'], "r", encoding="utf-8") as f:
            self.prompt_template = json.load(f).get(cfg["prompt_type"]).get("prompt")

    def _validate_campos(self):
        """Valida se a estrutura de campos está correta."""
        for i, c in enumerate(self.campos_a_extrair):
            if not isinstance(c, dict) or "campo" not in c or "descricao" not in c:
                raise ValueError(f"Estrutura inválida em campos_a_extrair[{i}]")

    def _build_prompt(self) -> str:
        """Constrói a string final do prompt injetando os campos e o texto no template."""
        campos_desc = "\n".join([f"{c['campo']}: {c['descricao']}" for c in self.campos_a_extrair])
        
        return self.prompt_template.format(
            campos_desc=campos_desc,
            clened_text=self.text_to_analyze
        )

    async def generate_regex_json(self) -> dict:
        """Chama o LLM usando o cliente nativo da OpenAI para gerar a lista JSON."""
        prompt = self._build_prompt()
        start_time = time.perf_counter()
        
        # IMPORTANTE PARA OPENAI JSON MODE:
        # Você DEVE instruir o modelo a usar JSON no system ou user prompt para evitar erros 400.
        messages = [
{
        "role": "user",
        #"content": """extract the data from this text and return a JSON list of objects with 'campo' and 'data' keys only."""
"content": 
"""

**ROLE:**
Você é um especialista em Engenharia de Dados e Expressões Regulares (Regex) em Python. Sua tarefa é analisar um texto de documento (OCR) e criar uma regex robusta para extrair um campo específico.

**OBJETIVO:**
Gerar uma expressão regular capaz de extrair o valor dos campos solicitados. A regex deve ser generalista o suficiente para funcionar em documentos similares, mas específica o suficiente para não pegar valores errados.

**INPUTS:**
1. `DOCUMENT_TEXT`: O texto completo extraído do documento.
2. `FIELD_NAME`: O nome do campo a ser extraído.
3. `FIELD_DESCRIPTION`: A descrição do que é esse campo.

**REGRA DE OURO:**
A regex DEVE usar "âncoras" — palavras-chave fixas que provavelmente sempre aparecerão perto do valor (ex: rótulos como "CPF:", "Data de Nasc.:", "Valor Total"). Não tente criar uma regex que dependa apenas do formato do valor se houver um rótulo claro próximo a ele.

**INSTRUÇÕES DE PENSAMENTO (Chain of Thought):**
Antes de gerar a regex final, siga estes passos:
1. **Identificação:** Localize onde o valor descrito em `FIELD_DESCRIPTION` está no `DOCUMENT_TEXT`.
2. **Análise de Âncoras:** O que vem imediatamente antes ou depois desse valor? (Ex: "Nome:", "R$"). Essas são suas âncoras.
3. **Padrão do Valor:** Qual é o formato do valor em si? (Ex: dígitos, datas, palavras em caixa alta).
4. **Construção da Regex:** Combine as âncoras e o padrão do valor em uma regex Python. Use grupos de captura `(...)` apenas para o valor que queremos extrair. Use `\s*` para lidar com variações de espaços.

**FORMATO DE SAÍDA (JSON):**
Retorne APENAS um JSON com a chave "campo" e o dado que é a regex de *TODOS OS CAMPOS*:
Ex:
{
  "nome": "<REGEX_AQUI>"
}

"""
    },
            {"role": "user", "content": prompt}
        ]
        
        
        
        print(messages)
        try:
            response = await self.client.chat.completions.create(
                model=self.model_name,
                messages=messages,
                # Ativa o modo JSON nativo da OpenAI.
                # Garante que o output seja um JSON válido.
                response_format={"type": "json_object"},
                temperature=1, # Recomendado 0 para tarefas de extração/código mais precisas
                reasoning_effort="minimal",
                verbosity="low"
            )
            
            end_time = time.perf_counter()
            duration = end_time - start_time
            
            json_output_str = response.choices[0].message.content
            json_output = json.loads(json_output_str)
            
            return {
                "duration": duration,
                "json_response": json_output,
                "model_name": self.model_name
            }
        
        except OpenAIError as e:
             return {"error": f"OpenAI API Error (model {self.model_name}): {e}", "model_name": self.model_name}
        except json.JSONDecodeError as e:
             return {"error": f"Falha ao decodificar JSON do modelo {self.model_name}: {e}. Output raw: {json_output_str}", "model_name": self.model_name}
        except Exception as e:
            return {"error": f"Unexpected Error (model {self.model_name}): {e}", "model_name": self.model_name}

    def extract_data_with_regex(self, regex_list: list) -> dict:
        """Aplica a lista de regexes gerada ao texto (Mantido idêntico ao original)."""
        extracted_data = {}
        
        # Garante que regex_list seja uma lista. 
        # Às vezes o modelo retorna um dict com uma chave contendo a lista, dependendo do prompt.
        # Ajuste conforme a estrutura real do seu prompt/schema.
        lista_para_iterar = regex_list
        if isinstance(regex_list, dict):
             # Tenta encontrar a primeira chave que contenha uma lista, caso o JSON venha aninhado
             for key, value in regex_list.items():
                 if isinstance(value, list):
                     lista_para_iterar = value
                     break

        if not isinstance(lista_para_iterar, list):
             return {"ERRO_ESTRUTURA": "O LLM não retornou uma lista de regexes válida.", "raw": regex_list}

        for item in lista_para_iterar:
            if not isinstance(item, dict): continue

            campo = item.get("campo")
            regex_str = item.get("regex_captura")

            if not campo: continue
            
            if regex_str is None: 
                extracted_data[campo] = None
                continue
                
            try:
                match = re.search(regex_str, self.text_to_analyze, re.MULTILINE | re.DOTALL)
                if match and match.lastindex and match.lastindex >= 1:
                    extracted_data[campo] = match.group(1).strip()
                else:
                    extracted_data[campo] = None
                    
            except re.error as e:
                # print(f"Erro na Regex para o campo '{campo}': {e}")
                extracted_data[campo] = f"ERRO_REGEX: {e}"
                
        return extracted_data

    async def run_extraction(self):
        """Orquestra a geração da Regex e a extração final dos dados."""
        llm_result = await self.generate_regex_json()
        
        if "error" in llm_result:
            return llm_result

        regex_list = llm_result["json_response"]
        final_data = self.extract_data_with_regex(regex_list)
        
        return {
            "model_name": self.model_name,
            "duration": llm_result["duration"],
            "regex_list": regex_list,
            "extracted_data": final_data
        }

async def main():
    # Dados de Input
    campos_a_extrair_input = [
        {"campo": "nome", "descricao": "Nome completo da pessoa"},
        {"campo": "endereco", "descricao": "Endereço residencial completo"},
        {"campo": "telefone", "descricao": "Número de telefone no formato internacional"},
    ]
    
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
    
    # --- ATENÇÃO: Use apenas nomes de modelos OpenAI válidos ---
    models_to_test = [
        "gpt-5-mini",
    ]
    
    # Carregamento do config.json (necessário manter se você usa arquivos externos para prompts)
    try:
        with open("config.json", "r", encoding="utf-8") as f:
            full_config = json.load(f)
            cfg = full_config.get("llm")
    except FileNotFoundError:
        print("❌ ERRO: 'config.json' não encontrado. Certifique-se que o arquivo existe.")
        return

    tasks = []
    # Mantemos uma lista de tuplas (nome_modelo, task) para saber quem é quem depois
    task_map = [] 

    for model_name_iter in models_to_test:
        # Cria uma cópia da config para não alterar a original se rodar em paralelo real
        current_cfg = cfg.copy()
        current_cfg['model_name'] = model_name_iter
        
        try:
            extractor = LLMExtractor(
                cfg=current_cfg,
                campos_a_extrair=campos_a_extrair_input,
                text_to_analyze=clened_text_input
            )
            task = asyncio.create_task(extractor.run_extraction())
            tasks.append(task)
            task_map.append((model_name_iter, task))
            
        except Exception as e:
            print(f"ERRO NA CRIAÇÃO DO EXTRATOR para '{model_name_iter}': {e}")

    if not tasks:
        print("Nenhuma tarefa foi iniciada.")
        return

    print(f"\nIniciando {len(tasks)} tarefas de extração em paralelo...\n" + "="*50)
    results = await asyncio.gather(*tasks, return_exceptions=True)
    print(results)
    for i, result in enumerate(results):
        model_used = task_map[i][0]
        print(f"\n--- Resultado para: {model_used} ---")

        if isinstance(result, Exception):
            print(f"❌ ERRO CRÍTICO NA EXECUÇÃO: {result}")
        elif "error" in result:
             print(f"❌ ERRO DA API: {result['error']}")
        else:
            duration_formatted = f"{result['duration']:.3f}"
            print(f"✅ Sucesso! Tempo total: {duration_formatted}s")
            
            print("\n📝 Regex Gerado:")
            print(json.dumps(result["regex_list"], indent=2, ensure_ascii=False))
            
            print("\n🚀 DADOS EXTRAÍDOS:")
            print(json.dumps(result["extracted_data"], indent=2, ensure_ascii=False))

    print("\n" + "=" * 50)
    print("Testes OpenAI concluídos.")

if __name__ == "__main__":
    asyncio.run(main())