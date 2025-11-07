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
            print(text)
            # Await the coroutine to get its result
            llm_extracted_data = await llm.extract_data_json()
            
            if "json_response" in llm_extracted_data:
                for field, data_obj in llm_extracted_data["json_response"].items():
                    if field in self.extracted_data:
                        self.extracted_data[field] = data_obj.get("dado")
                   
        return self.extracted_data