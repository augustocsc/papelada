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
        
        # MUDANÇA: Inicializa o dicionário de métricas
        self.metrics = {
            "llm_data_calls": 0,
            "llm_data_tokens": 0,
            "llm_data_time_s": 0.0,
            "llm_regex_calls": 0,
            "llm_regex_tokens": 0,
            "llm_regex_time_s": 0.0,
        }

        memory_rules = self.memory.get(self.label, {})
        self.known_rules = {key: memory_rules.get(key)
                            for key in self.extracted_data if key in memory_rules and key.endswith("_blacklist") == False}


    def _apply(self, text: str, rules: dict) -> dict:
        # ... (código _apply inalterado) ...
        for field, rule_or_rules in rules.items():
            patterns = rule_or_rules if isinstance(rule_or_rules, list) else [rule_or_rules]
            
            match_found = False
            for pattern in patterns:
                if not isinstance(pattern, str): continue
                match = re.search(pattern, text, re.MULTILINE | re.DOTALL)

                if match:
                    if match.groups(): group_content = match.group(1)
                    else: group_content = match.group(0)
                    if group_content is not None:
                        self.extracted_data[field] = group_content.strip()
                        print(f'Using known rule {pattern} on field {field}')
                        match_found = True
                        break
            
            if not match_found:
                self.extracted_data[field] = 'null'
        return self.extracted_data
    
    async def _background_regex_task(self, text: str):
        """ Executa a geração de regex E ATUALIZA MÉTRICAS com lock. """
        print(f"  Stage: [BG] Generating and validating regex rules for {self.label}")
        
        try:
            for field in self.extraction_schema.keys():
                
                blacklist_key = f"{field}_blacklist"
                current_blacklist = self.memory[self.label].get(blacklist_key, [])
                
                self.llm = LLMExtractor(
                    self.cfg["llm"], 
                    {field: self.extraction_schema[field]},
                    text, 
                    client=self.client,
                    failed_regexes=current_blacklist
                )
                
                # MUDANÇA: Captura tempo e dados da chamada LLM
                start_llm_regex_time = time.perf_counter()
                llm_extracted_rules = await self.llm.generate_regex_json()
                end_llm_regex_time = time.perf_counter()

                llm_time = end_llm_regex_time - start_llm_regex_time
                llm_tokens = llm_extracted_rules.get("usage", {}).get("total_tokens", 0)

                # ... (resto da lógica de validação da regex) ...
                if llm_extracted_rules and "json_response" in llm_extracted_rules:
                    rule_entry = llm_extracted_rules["json_response"].get(field)
                    
                    if rule_entry:
                        new_regex = rule_entry.get("regex")
                        expected_value = self.extraction_schema.get(field, {}).get("ref")
                        is_valid_rule = False
                        failure_reason = "Regex não encontrou correspondência (no match)."
                        
                        if not new_regex or not expected_value:
                             # Se a regex for nula, etc., ainda registamos a chamada e atualizamos as métricas
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
                            elif extracted_value:
                                failure_reason = f"Valor extraído ('{extracted_value}') não bate com o esperado ('{expected_value}')."
                        
                        except re.error as e:
                            print(f"⚠️  [BG] Invalid regex syntax for field '{field}': {new_regex}. Error: {e}")
                            failure_reason = f"Sintaxe de Regex inválida: {e}"
                            pass
                        
                        # MUDANÇA: Atualiza métricas e memória ATOMICAMENTE dentro do lock
                        async with self.lock:
                            # Atualiza métricas
                            self.metrics["llm_regex_calls"] += 1
                            self.metrics["llm_regex_time_s"] += llm_time
                            self.metrics["llm_regex_tokens"] += llm_tokens
                            
                            # Atualiza memória (lógica de blacklist/regras inalterada)
                            if is_valid_rule:
                                if field not in self.memory[self.label] or not isinstance(self.memory[self.label][field], list):
                                    self.memory[self.label][field] = []
                                if new_regex not in self.memory[self.label][field]:
                                    self.memory[self.label][field].append(new_regex)
                            else:
                                if blacklist_key not in self.memory[self.label] or not isinstance(self.memory[self.label][blacklist_key], list):
                                    self.memory[self.label][blacklist_key] = []
                                if new_regex:
                                    failed_rule_entry = {"regex": new_regex, "reason": failure_reason}
                                    if failed_rule_entry not in self.memory[self.label][blacklist_key]:
                                        self.memory[self.label][blacklist_key].append(failed_rule_entry)

            print(f"  Stage: [BG] Finished regex task for {self.label}.")
        except Exception as e:
            print(f"ERROR in background regex task for {self.label}: {e}")
    
    async def extract(self, text: str) -> tuple: 
        start_time = time.perf_counter()
        print(f"  Stage: Applying known rules for {self.label}")

        if self.known_rules:
            self.extracted_data = self._apply(text, self.known_rules)

        if 'null' in self.extracted_data.values():
            end_known_rules_time = time.perf_counter()
            print(f"  Stage: Applying known rules completed in {end_known_rules_time - start_time:.2f} seconds.")
            print(f"  Stage: Extracting data with LLM for {self.label}")
            
            fields_to_extract = [k for k, v in self.extracted_data.items() if v == 'null']
            self.extraction_schema = {k: self.extraction_schema[k] for k in fields_to_extract if k in self.extraction_schema}

            self.llm = LLMExtractor(self.cfg["llm"], self.extraction_schema, text, client=self.client)
            
            # MUDANÇA: Captura tempo e dados da chamada LLM
            start_llm_data_time = time.perf_counter()
            llm_extracted_data = await self.llm.extract_data_json()
            end_llm_data_time = time.perf_counter()
            
            # MUDANÇA: Atualiza métricas IMEDIATAMENTE (não precisa de lock aqui)
            self.metrics["llm_data_calls"] += 1
            self.metrics["llm_data_time_s"] += (end_llm_data_time - start_llm_data_time)
            self.metrics["llm_data_tokens"] += llm_extracted_data.get("usage", {}).get("total_tokens", 0)

            print(f"  Stage: LLM data extraction completed in {end_llm_data_time - start_llm_data_time:.2f} seconds.")
        
            background_task = None
            
            if self.mode != "standard":
                if "json_response" in llm_extracted_data:
                    valid_fields_for_regex_gen = {}
                    for field, data_obj in llm_extracted_data["json_response"].items():
                        if field in self.extracted_data:
                            if data_obj.get("confidence") != "low" and data_obj.get("dado") != "null":
                                self.extracted_data[field] = data_obj.get("dado")
                                valid_fields_for_regex_gen[field] = {
                                    "ref": data_obj.get("dado"),
                                    "description": self.extraction_schema.get(field)
                                }
                            else:
                                del self.extraction_schema[field]
                    
                    self.extraction_schema = valid_fields_for_regex_gen
                    
                    if self.extraction_schema:
                        background_task = asyncio.create_task(self._background_regex_task(text))
            
            # MUDANÇA: Não retorna mais métricas, elas estão em self.metrics
            return self.extracted_data, background_task
        
        # MUDANÇA: Não retorna mais métricas
        return self.extracted_data, None