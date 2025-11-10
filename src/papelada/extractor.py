import asyncio
import time
import json
import re
import os 
import dotenv
from dotenv import load_dotenv
from openai import AsyncOpenAI, OpenAIError
from .llm import LLMExtractor


class Extractor:
    def __init__(self, config: dict, file_schema: dict, shared_memory: dict, lock: asyncio.Lock, client: AsyncOpenAI, mode: str):
        self.cfg = config
        self.client = client
        self.mode = mode 

        self.extracted_data = {key: 'null' for key in file_schema["extraction_schema"]}
        self.extraction_schema = file_schema["extraction_schema"] # Este é o schema original (descrições)
        self.label = file_schema["label"]
        
        self.memory = shared_memory 
        self.lock = lock
        if self.label not in self.memory:
            self.memory[self.label] = {}
        
        self.metrics = {
            "llm_data_calls": 0,
            "llm_data_tokens": 0,
            "llm_data_time_s": 0.0,
            "llm_regex_calls": 0,
            "llm_regex_tokens": 0,
            "llm_regex_time_s": 0.0,
            "sync_data_extraction_time_s": 0.0,
            "async_rule_generation_time_s": 0.0,
            "total_processing_time_s": 0.0,
            "used_memory_rule": 0, # Métrica para o log
        }

        memory_rules = self.memory.get(self.label, {})
        self.known_rules = {}
        for key in self.extracted_data:
            rule_entry = memory_rules.get(key)
            if isinstance(rule_entry, str):
                self.known_rules[key] = rule_entry

    def _normalize_for_validation(self, text: str) -> str:
        """
        Helper para normalizar texto para uma validação robusta.
        Remove quebras de linha, tabulações e espaços múltiplos.
        """
        if not text:
            return ""
        # Substitui qualquer caractere de espaço em branco (incluindo \n, \t) por um único espaço
        text = re.sub(r'\s+', ' ', str(text)).strip()
        return text.lower() # Compara sempre em minúsculas

    def _apply(self, text: str, rules: dict) -> dict:
        """
        Aplica as regras de 'self.known_rules'.
        'rules' é agora um dict[str, str] (campo: "regex_string").
        """
        for field, pattern in rules.items():
            if not isinstance(pattern, str): continue
            
            try:
                match = re.search(pattern, text, re.MULTILINE | re.DOTALL)
                
                if match:
                    if match.groups():
                        group_content = match.group(1)
                    else:
                        group_content = match.group(0)
                    
                    if group_content is not None:
                        self.extracted_data[field] = group_content.strip()
                        self.metrics["used_memory_rule"] = 1 # Marca que usou regra
                        print(f"Using known rule {pattern} on field {field}")
            
            except re.error as e:
                print(f"Erro de Regex na regra conhecida para o campo '{field}': {e}. A regra será ignorada.")
        
        return self.extracted_data
    
    async def _background_regex_task(self, text: str, schema_for_learning: dict):
        """
        Gera e guarda regras de regex para o schema fornecido.
        MUDANÇA: Envia o schema completo (não mais um loop) para dar contexto ao LLM.
        """
        print(f"  Stage: [BG] Generating and validating regex rules for {self.label} (Schema: {list(schema_for_learning.keys())})")
        
        async_start_time = time.perf_counter()
        
        try:
            # --- MUDANÇA: O loop foi removido ---
            # 1. Pega os valores de referência (ground truth) da etapa de extração.
            expected_values = {field: data.get("ref_value") for field, data in schema_for_learning.items()}
            
            # 2. Se não tivermos um schema para aprender, saia.
            if not schema_for_learning:
                print(f"  [BG] Pulando aprendizado (schema de aprendizado vazio).")
                return

            self.llm = LLMExtractor(
                self.cfg["llm"], 
                schema_for_learning, # Passa o schema completo com 'ref_value' e 'description'
                text, 
                client=self.client
            )
            
            start_llm_regex_time = time.perf_counter()
            llm_extracted_rules = await self.llm.generate_regex_json()
            end_llm_regex_time = time.perf_counter()

            llm_time = end_llm_regex_time - start_llm_regex_time
            llm_tokens = llm_extracted_rules.get("usage", {}).get("total_tokens", 0)

            # Atualiza as métricas de LLM uma vez
            async with self.lock:
                self.metrics["llm_regex_calls"] += 1
                self.metrics["llm_regex_time_s"] += llm_time 
                self.metrics["llm_regex_tokens"] += llm_tokens

            if llm_extracted_rules and "json_response" in llm_extracted_rules:
                
                # 3. Itera sobre os resultados do LLM
                for field, rule_entry in llm_extracted_rules["json_response"].items():
                    
                    # 4. Verifica se este campo estava no nosso pedido original
                    if field not in schema_for_learning:
                        continue
                        
                    new_regex = rule_entry.get("regex")
                    expected_value = expected_values.get(field)
                    is_valid_rule = False
                    
                    if not new_regex or not expected_value:
                        print(f"  [BG] Regex ou valor esperado ausente para '{field}'. Pulando.")
                        continue

                    try:
                        match = re.search(new_regex, text, re.MULTILINE | re.DOTALL)
                        extracted_value = None
                        if match:
                            if match.groups(): group_content = match.group(1)
                            else: group_content = match.group(0)
                            if group_content is not None: extracted_value = group_content.strip()

                        # --- MUDANÇA CRÍTICA: Normalizar antes de comparar ---
                        norm_extracted = self._normalize_for_validation(extracted_value)
                        norm_expected = self._normalize_for_validation(expected_value)

                        if norm_extracted and norm_extracted == norm_expected:
                            is_valid_rule = True
                        else:
                            print(f"  [BG] Validação falhou para '{field}'.")
                            print(f"     -> Esperado (Norm): '{norm_expected}'")
                            print(f"     -> Regex obteve (Norm): '{norm_extracted}'")
                            # Log bruto para depuração (opcional)
                            # print(f"     -> Esperado (Bruto): '{expected_value}'")
                            # print(f"     -> Regex obteve (Bruto): '{extracted_value}'")
                    
                    except re.error as e:
                        print(f"⚠️  [BG] Invalid regex syntax for field '{field}': {new_regex}. Error: {e}")
                        pass
                    
                    # 6. Salva na memória (se for válido)
                    if is_valid_rule:
                        async with self.lock:
                            print(f"  [BG] New rule validated and saved for '{field}'.")
                            self.memory[self.label][field] = new_regex
                    else:
                        print(f"  [BG] Generated rule for '{field}' failed validation.")
                        pass # Não salva a regra
            
            elif "error" in llm_extracted_rules:
                 print(f"  [!] LLM regex generation failed: {llm_extracted_rules['error']}")
                 
            # --- FIM DA MUDANÇA ---

            print(f"  Stage: [BG] Finished regex task for {self.label}.")
        except Exception as e:
            print(f"ERROR in background regex task for {self.label}: {e}")
        finally:
            async_end_time = time.perf_counter()
            total_async_duration = async_end_time - async_start_time
            
            async with self.lock:
                self.metrics["async_rule_generation_time_s"] = total_async_duration
                self.metrics["total_processing_time_s"] = self.metrics.get("sync_data_extraction_time_s", 0) + total_async_duration
    
    async def extract(self, text: str, reusable_fields: set) -> tuple: 
        sync_start_time = time.perf_counter()
        
        print(f"  Stage: Applying known rules for {self.label}")

        if self.known_rules:
            self.extracted_data = self._apply(text, self.known_rules)

        background_task = None
        # Este dicionário conterá o schema APENAS para os campos que precisam aprender
        valid_fields_for_regex_gen = {}

        # Se algum campo ainda for 'null' APÓS aplicar as regras de memória
        if 'null' in self.extracted_data.values():
            
            # Se o cliente LLM não foi fornecido (ex: chave ausente), não podemos fazer mais nada.
            if self.client is None:
                print(f"  [!] LLM client is None. Skipping LLM extraction for {self.label}.")
                sync_end_time = time.perf_counter()
                self.metrics["sync_data_extraction_time_s"] = sync_end_time - sync_start_time
                self.metrics["total_processing_time_s"] = self.metrics["sync_data_extraction_time_s"]
                return self.extracted_data, None # Retorna o que foi pego da memória (ou nada)

            end_known_rules_time = time.perf_counter()
            print(f"  Stage: Applying known rules completed in {end_known_rules_time - sync_start_time:.2f} seconds.")
            print(f"  Stage: Extracting data with LLM for {self.label}")
            
            fields_to_extract = [k for k, v in self.extracted_data.items() if v == 'null']
            # Usa 'self.extraction_schema' (o original) para obter as descrições
            current_extraction_schema = {k: self.extraction_schema[k] for k in fields_to_extract if k in self.extraction_schema}

            self.llm = LLMExtractor(self.cfg["llm"], current_extraction_schema, text, client=self.client)
            
            start_llm_data_time = time.perf_counter()
            llm_extracted_data = await self.llm.extract_data_json()
            end_llm_data_time = time.perf_counter()
            
            self.metrics["llm_data_calls"] += 1
            self.metrics["llm_data_time_s"] += (end_llm_data_time - start_llm_data_time)
            self.metrics["llm_data_tokens"] += llm_extracted_data.get("usage", {}).get("total_tokens", 0)

            print(f"  Stage: LLM data extraction completed in {end_llm_data_time - start_llm_data_time:.2f} seconds.")
        
            if "json_response" in llm_extracted_data:
                
                for field, data_obj in llm_extracted_data.get("json_response", {}).items():
                    # Verifica se o campo é um dos que estávamos à espera
                    if field in current_extraction_schema: 
                        
                        dado_extraido = data_obj.get("dado")
                        confianca = data_obj.get("confidence")

                        is_valid_for_learning = (
                            confianca != "low" and
                            dado_extraido is not None and
                            str(dado_extraido).strip().lower() != "null"
                        )
                        
                        # Atualiza o 'self.extracted_data' com o valor da LLM
                        if dado_extraido is not None:
                           self.extracted_data[field] = dado_extraido
                        
                        # Agora, verifica se deve preparar para aprender
                        if is_valid_for_learning:
                            if field not in self.known_rules and field in reusable_fields:
                                # Prepara o "ground truth" para a tarefa de background
                                # --- MUDANÇA CRÍTICA ---
                                valid_fields_for_regex_gen[field] = {
                                    "ref_value": dado_extraido, # <--- Corrigido de "ref" para "ref_value"
                                    "description": self.extraction_schema.get(field) # Pega a descrição original
                                }
                                # --- FIM DA MUDANÇA ---
            
            elif "error" in llm_extracted_data:
                print(f"  [!] LLM data extraction failed: {llm_extracted_data['error']}")
                

        # Agora, o bloco 'if self.mode != "standard"' protege APENAS
        # a *criação* da task de fundo.
        if self.mode != "standard":
            # 'valid_fields_for_regex_gen' foi preenchido acima
            if valid_fields_for_regex_gen:
                # MUDANÇA: Passa o schema de aprendizado explicitamente para a task
                background_task = asyncio.create_task(self._background_regex_task(text, valid_fields_for_regex_gen))
        
        sync_end_time = time.perf_counter()
        self.metrics["sync_data_extraction_time_s"] = sync_end_time - sync_start_time
        self.metrics["total_processing_time_s"] = self.metrics["sync_data_extraction_time_s"]

        return self.extracted_data, background_task