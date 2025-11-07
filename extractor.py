import asyncio
import time
import json
import re
import os # Importe o módulo os para acessar as variáveis de ambiente
import dotenv
from dotenv import load_dotenv
from openai import AsyncOpenAI, OpenAIError
from llm import LLMExtractor
# Carrega variáveis de ambiente (.env)
load_dotenv()

# Verifica se a chave da OpenAI está disponível
if not os.getenv("OPENAI_API_KEY"):
    print("⚠️ AVISO: OPENAI_API_KEY não encontrada nas variáveis de ambiente.")
client = AsyncOpenAI()


class Extractor:
    def __init__(self, config: dict, file_schema: dict):
        self.cfg = config

        # Creating a list of fields to extract
        self.extracted_data = {key: 'null' for key in file_schema["extraction_schema"]}
        self.extraction_schema = file_schema["extraction_schema"]
        
        self.memory = self.load_memory(config.get("memory_file"))
        self.rules = {}
        
        self.known_rules = {}
        # Check if there are known rules in memory for this label
        if file_schema["label"] in self.memory:
            memory_rules = self.memory[file_schema["label"]] # If there are, load them
            self.known_rules = {key: memory_rules[key] 
                                    for key in self.extracted_data if key in memory_rules}

    def load_memory(self, path: str) -> dict:
        try:
            with open(path, 'r', encoding='utf-8') as f:
                memory_data = json.load(f)
                return memory_data
        except FileNotFoundError:
            print(f"Memory file not found at {path}. Initializing empty memory.")
            return {}

    def _apply(self, text: str, rules: dict) -> dict:
        
        for field, rule in rules.items():
            pattern = rule
            match = re.search(pattern, text, re.MULTILINE | re.DOTALL)
            if match:
                self.extracted_data[field] = match.group(1).strip()
                #if returned more than expected
            else:
                self.extracted_data[field] = 'null'
        return self.extracted_data
    
    async def extract(self, text: str) -> dict: # Changed to async def

        if self.known_rules:
            self.extracted_data = self._apply(text, self.known_rules)

        if 'null' in self.extracted_data.values():
            # remove filds already extracted
            fields_to_extract = [k for k, v in self.extracted_data.items() if v == 'null']
            self.extraction_schema = {k: self.extraction_schema[k] for k in fields_to_extract if k in self.extraction_schema}

            llm = LLMExtractor(self.cfg["llm"], self.extraction_schema, text, client=client)
            
            # Await the coroutine to get its result
            llm_extracted_data = await llm.extract_data_json()
            
            if "json_response" in llm_extracted_data:
                for field, data_obj in llm_extracted_data["json_response"].items():
                    if field in self.extracted_data:
                        self.extracted_data[field] = data_obj.get("dado")
                   
        return self.extracted_data
    
    
    
# # --- 2. DEFINIÇÃO DO SCHEMA DE SAÍDA (Agnóstico e com 2 Regex) ---
# # Schema para um único item da lista (representa um campo)
# item_schema = types.Schema(
#     type=types.Type.OBJECT,
#     properties={
#         "campo": types.Schema(
#             type=types.Type.STRING, 
#             description="Nome técnico do campo (ex: 'nome')."
#         ),
#         "regex_captura": types.Schema(
#             type=types.Type.STRING,
#             description="A string da EXPRESSÃO REGULAR com um grupo de captura (...) para extrair o valor. Deve ser null (como valor, não a string 'null') se não for encontrado."
#         ),
#     },
#     required=["campo", "regex_captura"]
# )

# # Schema final é uma lista (ARRAY) de itens
# response_schema = types.Schema(
#     type=types.Type.ARRAY,
#     items=item_schema
# )
# # --- 3. PROMPT GENERALISTA ---
# # Validate campos_a_extrair structure to provide a clearer error if a key is missing
# def validate_campos(campos):
#     for i, c in enumerate(campos):
#         if not isinstance(c, dict):
#             raise TypeError(f"campos_a_extrair[{i}] is not a dict: {c!r}")
#         missing = [k for k in ("campo", "descricao") if k not in c]
#         if missing:
#             raise KeyError(f"campos_a_extrair[{i}] is missing keys: {missing}. Item: {c!r}")

# validate_campos(campos_a_extrair)

# campos_desc = "\n".join([f"- {c['campo']} ({c['descricao']})" for c in campos_a_extrair])


# main_prompt = f"""
# Você é um Expert em Extração de Dados e Expressões Regulares (Regex) para Python.
# Sua tarefa é gerar uma LISTA JSON (ARRAY) de objetos, onde cada objeto mapeia um campo para sua regex de extração.
# O texto não é estruturado, então o dado pode não estar logo ao lado do rótulo, análise todo contexto para cada campo.

# **REGRAS CRÍTICAS E BOAS PRÁTICAS:**

# 1.  **FORMATO JSON**: Gere ESTRITAMENTE uma lista JSON (ARRAY).
# 2.  ** Questione a descrição, garanta que entendeu o campo antes de gerar a regex. **
# 3.  **OBJETO NA LISTA**: Cada objeto na lista deve ter DUAS chaves:
#     * `"campo"`: O nome técnico do campo (ex: "nome").
#     * `"regex_captura"`: A string de regex.
# 4. **CHAVE "regex_captura"**: O valor desta chave deve ser uma string de regex que:
#     * **USE GRUPOS DE CAPTURA**: A regex DEVE usar um grupo de captura `(...)` para isolar APENAS o valor a ser extraído.
#     * **USE CONTEXTO (ÂNCORAS)**: A regex DEVE usar texto-âncora (rótulos) ao redor do valor para localizá-lo com segurança.
#     * ** IGNORAR RÓTULOS INTERMEDIÁRIOS**: O valor de um campo (ex: 'Valor A') pode não estar imediatamente após seu rótulo ('Rótulo A'). Pode estar após o rótulo de outro campo ('Rótulo B'). A regex deve ser robusta para "pular" outros rótulos conhecidos que possam aparecer entre o rótulo-âncora e o valor-alvo.
#         * **Exemplo de Texto:** `Rótulo A\nRótulo B\nValor A`
#         * **Regex Correta para "Rótulo A":** `Rótulo A\s*(?:Rótulo B\s*)?([\s\S]+?)` (A regex pula o 'Rótulo B' para capturar o 'Valor A').
#     * ** DELIMITAÇÃO DE CAMPOS MULTI-LINHA**: Para campos cujo valor pode se estender por múltiplas linhas (ex: descrições, endereços), o grupo de captura deve ser não-ganancioso (ex: `[\s\S]+?`). 
#     * A captura DEVE se estender até encontrar o **início do rótulo-âncora de um campo subsequente** conhecido.
#     * **NÃO USE Padrões de Dados como Delimitadores**: A regex NÃO DEVE parar a captura ao encontrar um padrão de dados (como padrões numéricos, datas ou códigos). Esses padrões devem ser considerados **parte do valor** a ser capturado. O único delimitador de fim confiável é o rótulo de outro campo.
#     * Se o campo for conhecido como endereço ou nome, imponha um tamanho mínimo razoável
# 5. **[REGRA REFORÇADA] DADOS AUSENTES (NULO vs. VAZIO)**: Analise o texto entre o rótulo-âncora (ex: 'Telefone Profissional') e o rótulo-âncora do *próximo* campo (ex: 'SITUAÇÃO'). Se, nesse intervalo, NÃO houver um valor que corresponda ao padrão esperado (ex: um número de telefone), o valor de "regex_captura" DEVE ser `null` (o JSON nulo).
#     * **NÃO gere uma regex opcional** (como `(...)?`) que possa capturar uma string vazia (`""`) se o dado estiver ausente. A regex deve falhar ou ser `null`.

# 6.  **NÃO USE VALORES LITERAIS**: NÃO inclua os valores exatos na regex. Use padrões genéricos.

# **CAMPOS REQUERIDOS:**
# {campos_desc}

# **TEXTO A SER ANALISADO:**
# {cleaned_text}

# """
# # --- 4. CONFIGURAÇÃO FINAL ---

# EXTRACTION_CONFIG = types.GenerateContentConfig(
#     thinking_config=types.ThinkingConfig(thinking_budget=0), 
#     response_mime_type="application/json",
#     response_schema=response_schema,
#     temperature=1,
# )


# # --- 5. FUNÇÃO DE EXECUÇÃO ---

# async def generate_regex_json(prompt: str):
#     start_time = time.perf_counter()
    
#     try:
#         response = await client.aio.models.generate_content(
#             model="gemini-2.5-flash", 
#             contents=prompt,
#             config=EXTRACTION_CONFIG
#         )
        
#         end_time = time.perf_counter()
#         duration = end_time - start_time
        
#         # O modelo deve retornar a resposta como uma string JSON válida
#         json_output = json.loads(response.text)
        
#         return {
#             "duration": duration,
#             "json_response": json_output
#         }
    
#     except APIError as e:
#         return {"error": f"API Error: {e}"}
#     except json.JSONDecodeError as e:
#         return {"error": f"Failed to decode JSON from response. Details: {e}. Raw response: {response.text}"}
#     except Exception as e:
#         return {"error": f"An unexpected error occurred: {e}"}

# def extract_data_with_regex(text: str, regex_list: list) -> dict:
#     """
#     Aplica a lista de regexes gerada ao texto para extrair os dados.
#     """
#     extracted_data = {}
    
#     # Itera sobre a LISTA de objetos
#     for item in regex_list:
#         campo = item.get("campo")
#         regex_str = item.get("regex_captura")

#         if not campo:
#             continue # Ignora item malformado
        
#         # O modelo retornará 'null' (None) para campos não encontrados
#         if not regex_str: 
#             extracted_data[campo] = None
#             continue
            
#         try:
#             # re.MULTILINE faz ^ e $ funcionarem em cada linha
#             # re.DOTALL faz o '.' corresponder a quebras de linha (bom para endereços)
#             match = re.search(regex_str, text, re.MULTILINE | re.DOTALL)
            
#             if match:
#                 # Pega o primeiro grupo de captura
#                 extracted_data[campo] = match.group(1).strip()
#             else:
#                 extracted_data[campo] = None
                
#         except re.error as e:
#             print(f"Erro na Regex para o campo '{campo}': {e}")
#             extracted_data[campo] = f"ERRO_REGEX: {e}"
#         except AttributeError:
#              # Isso pode acontecer se a regex não tiver um grupo de captura
#              print(f"Erro de Atributo (Regex sem grupo?) para o campo '{campo}'")
#              extracted_data[campo] = "ERRO_REGEX: Sem grupo de captura"
            
#     return extracted_data

# # --- 6. EXECUÇÃO PRINCIPAL ---

# async def main():
        
#     result = await generate_regex_json(main_prompt)
#     print(main_prompt)
#     print("\n" + "=" * 80)
    
#     if "error" in result:
#         print(f"❌ ERRO AO GERAR REGEX: {result['error']}")
#     else:
#         duration_formatted = f"{result['duration']:.3f}"
#         print(f"✅ Regexes geradas com sucesso. Tempo: {duration_formatted}s")
        
#         # A resposta JSON agora é uma LISTA
#         regex_list = result["json_response"]
        
#         print("\n📝 JSON de Regex Gerado:")
#         print(json.dumps(regex_list, indent=4, ensure_ascii=False))
        
#         print("\n" + "=" * 80)
#         print("🚀 Executando extração com as regexes geradas...")
        
#         # --- A ETAPA FALTANTE (Atualizada) ---
#         final_data = extract_data_with_regex(cleaned_text, regex_list)
#         # ------------------------------------
        
#         print("\n✅ DADOS EXTRAÍDOS:")
#         print(json.dumps(final_data, indent=4, ensure_ascii=False))
    
#     print("\n" + "=" * 80)

#     # Fechar o cliente assíncrono
#     await client.aio.aclose()


# if __name__ == "__main__":
#     asyncio.run(main())