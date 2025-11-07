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
        self.label = file_schema["label"]
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
                content = f.read()
                if not content:
                    return {}
                memory_data = json.loads(content)
                return memory_data
        except FileNotFoundError:
            print(f"Memory file not found at {path}. Initializing empty memory.")
            return {}

    def _save_memory(self, path: str):
        
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(self.memory, f, indent=2, ensure_ascii=False)

    def _apply(self, text: str, rules: dict) -> dict:
        for field, rule_or_rules in rules.items():
            patterns = rule_or_rules if isinstance(rule_or_rules, list) else [rule_or_rules]
            
            match_found = False
            for pattern in patterns:
                if not isinstance(pattern, str): continue

                match = re.search(pattern, text, re.MULTILINE | re.DOTALL)

                if match:
                    group_content = match.group(1)
                    if group_content is not None:
                        self.extracted_data[field] = group_content.strip()
                        match_found = True
                        break
            
            if not match_found:
                self.extracted_data[field] = 'null'
        return self.extracted_data
    
    async def extract(self, text: str) -> dict: # Changed to async def

        if self.known_rules:
            self.extracted_data = self._apply(text, self.known_rules)

        if 'null' in self.extracted_data.values():
            # remove filds already extracted
            fields_to_extract = [k for k, v in self.extracted_data.items() if v == 'null']
            self.extraction_schema = {k: self.extraction_schema[k] for k in fields_to_extract if k in self.extraction_schema}

            self.llm = LLMExtractor(self.cfg["llm"], self.extraction_schema, text, client=client)
            
            # Await the coroutine to get its result
            llm_extracted_data = await self.llm.extract_data_json()
            print(json.dumps(llm_extracted_data["json_response"], indent=2, ensure_ascii=False))
            extract_rules = {}
            print(f"extract rules: {json.dumps(self.extraction_schema, indent=2, ensure_ascii=False)}")
            
            
            if "json_response" in llm_extracted_data and self.cfg["mode"] == "smart":
                for field, data_obj in llm_extracted_data["json_response"].items():
                    if field in self.extracted_data:
                        if data_obj.get("confidence") != "low" and data_obj.get("dado") != "null":
                            self.extracted_data[field] = data_obj.get("dado")
                            self.extraction_schema[field] = {
                                "ref": data_obj.get("dado"),
                                "description": self.extraction_schema.get(field)
                            }
                        else:
                            del self.extraction_schema[field]
                        
                print(f"extract rules: {json.dumps(self.extraction_schema, indent=2, ensure_ascii=False)}")
                llm_extracted_rules = await self.llm.generate_regex_json()
                print(f"{json.dumps(llm_extracted_rules['json_response'], indent=2, ensure_ascii=False)}")

                if llm_extracted_rules and "json_response" in llm_extracted_rules and extract_rules:
                    label = self.label
                    if label not in self.memory:
                        self.memory[label] = {}
                    
                    for field, rule in llm_extracted_rules["json_response"].items():
                        if field in self.memory[label]:
                            if isinstance(self.memory[label][field], list):
                                self.memory[label][field].append(rule["regex"])
                            else:
                                # If it's not a list, create one with the existing and new rule
                                self.memory[label][field] = [self.memory[label][field], rule['regex']]
                        else:
                            # If the field doesn't exist, create a new list with the rule
                            self.memory[label][field] = [rule["regex"]]
                    
                self._save_memory(self.cfg.get("memory_file"))

                   
        return self.extracted_data