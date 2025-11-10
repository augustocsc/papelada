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
        self.extraction_schema = file_schema["extraction_schema"]
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
        }

        memory_rules = self.memory.get(self.label, {})
        self.known_rules = {}
        for key in self.extracted_data:
            rule_entry = memory_rules.get(key)
            if isinstance(rule_entry, str):
                self.known_rules[key] = rule_entry


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
                        print(f'Using known rule {pattern} on field {field}')
            
            except re.error as e:
                print(f"Erro de Regex na regra conhecida para o campo '{field}': {e}. A regra será ignorada.")
        
        return self.extracted_data
    
    async def _background_regex_task(self, text: str):
        """
        Gera e guarda uma *única* regra de regex se ela for validada.
        """
        print(f"  Stage: [BG] Generating and validating regex rules for {self.label}")
        
        async_start_time = time.perf_counter()
        total_llm_regex_time_this_task = 0.0

        try:
            for field in self.extraction_schema.keys():
                
                self.llm = LLMExtractor(
                    self.cfg["llm"], 
                    {field: self.extraction_schema[field]},
                    text, 
                    client=self.client
                )
                
                start_llm_regex_time = time.perf_counter()
                llm_extracted_rules = await self.llm.generate_regex_json()
                end_llm_regex_time = time.perf_counter()

                llm_time = end_llm_regex_time - start_llm_regex_time
                llm_tokens = llm_extracted_rules.get("usage", {}).get("total_tokens", 0)
                total_llm_regex_time_this_task += llm_time

                if llm_extracted_rules and "json_response" in llm_extracted_rules:
                    rule_entry = llm_extracted_rules["json_response"].get(field)
                    
                    if rule_entry:
                        new_regex = rule_entry.get("regex")
                        expected_value = self.extraction_schema.get(field, {}).get("ref")
                        is_valid_rule = False
                        
                        if not new_regex or not expected_value:
                            async with self.lock:
                                self.metrics["llm_regex_calls"] += 1
                                self.metrics["llm_regex_time_s"] += llm_time 
                                self.metrics["llm_regex_tokens"] += llm_tokens
                            continue

                        try:
                            match = re.search(new_regex, text, re.MULTILINE | re.DOTALL)
                            extracted_value = None
                            if match:
                                if match.groups(): group_content = match.group(1)
                                else: group_content = match.group(0)
                                if group_content is not None: extracted_value = group_content.strip()

                            if extracted_value and extracted_value == expected_value:
                                is_valid_rule = True
                        
                        except re.error as e:
                            print(f"⚠️  [BG] Invalid regex syntax for field '{field}': {new_regex}. Error: {e}")
                            pass
                        
                        async with self.lock:
                            self.metrics["llm_regex_calls"] += 1
                            self.metrics["llm_regex_time_s"] += llm_time 
                            self.metrics["llm_regex_tokens"] += llm_tokens
                            
                            if is_valid_rule:
                                print(f"  [BG] New rule validated and saved for '{field}'.")
                                self.memory[self.label][field] = new_regex
                            else:
                                print(f"  [BG] Generated rule for '{field}' failed validation.")
                                pass

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
        valid_fields_for_regex_gen = {}

        if 'null' in self.extracted_data.values():
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
        
            # --- MUDANÇA (Correção do Bug) ---
            # Este bloco de análise de resposta foi MOVIDO para fora
            # e para cima do 'if self.mode != "standard"'.
            # Ele agora é executado em TODOS os modos.
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
                        # se for "None" ou "null", ele permanece "null" (graças ao .get("dado"))
                        if dado_extraido is not None:
                           self.extracted_data[field] = dado_extraido
                        
                        # Agora, verifica se deve preparar para aprender
                        if is_valid_for_learning:
                            if field not in self.known_rules and field in reusable_fields:
                                valid_fields_for_regex_gen[field] = {
                                    "ref": dado_extraido,
                                    "description": self.extraction_schema.get(field)
                                }

        # --- FIM DA MUDANÇA ---

        # Agora, o bloco 'if self.mode != "standard"' protege APENAS
        # a *criação* da task de fundo.
        if self.mode != "standard":
            # 'valid_fields_for_regex_gen' foi preenchido acima
            if valid_fields_for_regex_gen:
                # Atualiza o schema (que a task de fundo vai ler)
                self.extraction_schema = valid_fields_for_regex_gen
                background_task = asyncio.create_task(self._background_regex_task(text))
        
        sync_end_time = time.perf_counter()
        self.metrics["sync_data_extraction_time_s"] = sync_end_time - sync_start_time
        self.metrics["total_processing_time_s"] = self.metrics["sync_data_extraction_time_s"]

        return self.extracted_data, background_task