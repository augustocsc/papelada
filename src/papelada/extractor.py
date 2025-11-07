import asyncio
import time
import json
import re
import os # Importe o módulo os para acessar as variáveis de ambiente
import dotenv
from dotenv import load_dotenv
from openai import AsyncOpenAI, OpenAIError
from .llm import LLMExtractor


class Extractor:
    def __init__(self, config: dict, file_schema: dict, shared_memory: dict, lock: asyncio.Lock, client: AsyncOpenAI): # <-- Recebe o client
        self.cfg = config
        self.client = client

        # Creating a list of fields to extract
        self.extracted_data = {key: 'null' for key in file_schema["extraction_schema"]}
        self.extraction_schema = file_schema["extraction_schema"]
        self.label = file_schema["label"]
        
        # --- MODIFICADO: Memória compartilhada e Lock ---
        self.memory = shared_memory 
        self.lock = lock
        # Garante que o label exista na memória e tenha estrutura de blacklist
        if self.label not in self.memory:
            self.memory[self.label] = {}
        # --- FIM DA MODIFICAÇÃO ---

        self.rules = {}
        
        self.known_rules = {}
        # Check if there are known rules in memory for this label
        memory_rules = self.memory.get(self.label, {})
        self.known_rules = {key: memory_rules.get(key)
                            for key in self.extracted_data if key in memory_rules and key.endswith("_blacklist") == False}


    # O método load_memory foi movido para main.py
    # O método _save_memory foi movido para dentro do Lock na tarefa de fundo para salvar o objeto 'self.memory' compartilhado
    def _save_memory(self, path: str):
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(self.memory, f, indent=2, ensure_ascii=False)

    def _apply(self, text: str, rules: dict) -> dict:
        for field, rule_or_rules in rules.items():
            patterns = rule_or_rules if isinstance(rule_or_rules, list) else [rule_or_rules]
            
            match_found = False
            for pattern in patterns:
                if not isinstance(pattern, str): continue

                # O padrão regex pode ter vindo de uma lista de regras (não-blacklist), 
                # então o tratamento é o mesmo.
                
                match = re.search(pattern, text, re.MULTILINE | re.DOTALL)

                if match:
                    if match.groups():
                        group_content = match.group(1)
                    else:
                        group_content = match.group(0)
                    if group_content is not None:
                        self.extracted_data[field] = group_content.strip()
                        print(f'Using known rule {pattern} on field {field}')  # Imprime a regra e o campo quando aplicado
                        match_found = True
                        break
            
            if not match_found:
                self.extracted_data[field] = 'null'
        return self.extracted_data
    
    # --- NOVO MÉTODO PARA TAREFA DE FUNDO ---
    async def _background_regex_task(self, text: str):
        """
        Executa a geração, validação e salvamento de regex em segundo plano.
        """
        print(f"  Stage: [BG] Generating and validating regex rules for {self.label}")
        
        try:
            # Prepara a lista de campos para a LLMExtractor.
            # O campo 'extraction_schema' já contém apenas os campos que precisam de regex.
            for field in self.extraction_schema.keys():
                
                # --- Lógica da Blacklist ---
                # Pega a blacklist para o campo atual
                blacklist_key = f"{field}_blacklist"
                current_blacklist = self.memory[self.label].get(blacklist_key, [])
                
                # Instancia LLMExtractor passando a blacklist
                self.llm = LLMExtractor(
                    self.cfg["llm"], 
                    {field: self.extraction_schema[field]}, # Passa apenas o schema do campo atual
                    text, 
                    client=self.client,
                    failed_regexes=current_blacklist
                )
                
                # Gera regex para um único campo
                llm_extracted_rules = await self.llm.generate_regex_json()
                
                if llm_extracted_rules and "json_response" in llm_extracted_rules:
                    rule_entry = llm_extracted_rules["json_response"].get(field)
                    
                    if rule_entry:
                        new_regex = rule_entry.get("regex")
                        expected_value = self.extraction_schema.get(field, {}).get("ref")

                        is_valid_rule = False
                        
                        if not new_regex or not expected_value:
                            continue

                        try:
                            match = re.search(new_regex, text, re.MULTILINE | re.DOTALL)
                            extracted_value = None
                            if match:
                                if match.groups():
                                    group_content = match.group(1)
                                else:
                                    group_content = match.group(0)
                                
                                if group_content is not None:
                                    extracted_value = group_content.strip()

                            if extracted_value and extracted_value == expected_value:
                                is_valid_rule = True
                        
                        except re.error as e:
                            print(f"⚠️  [BG] Invalid regex syntax for field '{field}': {new_regex}. Error: {e}")
                            pass
                        
                        # --- MODIFICAÇÃO DE SALVAMENTO COM BLACKLIST/LOCK ---
                        # Adquire o "cadeado" antes de modificar e salvar. Isso garante que a memória
                        # será modificada em um único passo atômico.
                        async with self.lock:
                            if is_valid_rule:
                                # Adiciona a regra à lista de regras válidas
                                if field not in self.memory[self.label] or not isinstance(self.memory[self.label][field], list):
                                    self.memory[self.label][field] = []
                                
                                if new_regex not in self.memory[self.label][field]:
                                    self.memory[self.label][field].append(new_regex)
                                
                                # Remove o campo da blacklist se ele estava lá
                                if new_regex in self.memory[self.label].get(blacklist_key, []):
                                    self.memory[self.label][blacklist_key.remove(new_regex)]
                            
                            else:
                                # Adiciona a regra inválida à blacklist (se não estiver lá)
                                if blacklist_key not in self.memory[self.label] or not isinstance(self.memory[self.label][blacklist_key], list):
                                    self.memory[self.label][blacklist_key] = []

                                if new_regex and new_regex not in self.memory[self.label][blacklist_key]:
                                    self.memory[self.label][blacklist_key].append(new_regex)
                            
                            # Salva a memória após cada iteração de campo (dentro do lock) para
                            # que processos paralelos possam ler as regras mais recentes.
                            self._save_memory(self.cfg.get("memory_file")) 

            print(f"  Stage: [BG] Finished regex task for {self.label}.")
        except Exception as e:
            print(f"ERROR in background regex task for {self.label}: {e}")
    
    # --- MÉTODO EXTRACT MODIFICADO ---
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

            # A LLMExtractor para extração de dados não precisa de blacklist, apenas os dados a extrair.
            self.llm = LLMExtractor(self.cfg["llm"], self.extraction_schema, text, client=self.client)
            
            llm_extracted_data = await self.llm.extract_data_json()
            end_llm_data_time = time.perf_counter()
            print(f"  Stage: LLM data extraction completed in {end_llm_data_time - end_known_rules_time:.2f} seconds.")
        
            background_task = None
            
            if "json_response" in llm_extracted_data and self.cfg["mode"] == "smart" or self.cfg["mode"] == "pro":
                # Processa os dados extraídos, preparando o 'extraction_schema' para a tarefa de fundo
                valid_fields_for_regex_gen = {}
                for field, data_obj in llm_extracted_data["json_response"].items():
                    if field in self.extracted_data:
                        if data_obj.get("confidence") != "low" and data_obj.get("dado") != "null":
                            self.extracted_data[field] = data_obj.get("dado")
                            
                            # Prepara o schema de referência APENAS para campos bem extraídos
                            valid_fields_for_regex_gen[field] = {
                                "ref": data_obj.get("dado"),
                                "description": self.extraction_schema.get(field)
                            }
                        else:
                            # Se a confiança for baixa/dado nulo, remove do schema
                            del self.extraction_schema[field]
                
                # Atualiza o schema com apenas os campos que serão usados para gerar regex
                self.extraction_schema = valid_fields_for_regex_gen
                
                # Cria a tarefa de fundo SE houver campos para gerar regex
                if self.extraction_schema:
                    background_task = asyncio.create_task(self._background_regex_task(text))
            
            return self.extracted_data, background_task
        
        return self.extracted_data, None