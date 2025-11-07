import asyncio
import time
import json
import re
import os
from dotenv import load_dotenv

# --- 1. Importações e Configuração de Base ---
from openai import AsyncOpenAI, OpenAIError

class LLMExtractor:
    """
    Encapsula a lógica para geração de Regex estruturada via LLM (OpenAI Nativo)
    e a subsequente aplicação das regexes para extração de dados.
    """

    def __init__(self, cfg: dict, campos_a_extrair: list, text_to_analyze: str, client=None, failed_regexes: list = None):
        """
        Inicializa o extrator, carregando o schema e o prompt.
        """
        self.model_name = cfg['model_name']

        if client is None:
            raise ValueError("Client cannot be None. Please provide an AsyncOpenAI client instance.")
        self.client = client
        
        self.campos_a_extrair = campos_a_extrair
        self.text_to_analyze = text_to_analyze
        self.failed_regexes = failed_regexes if failed_regexes is not None else [] # Aceita failed_regexes
        
        #Load prompts and configs for data extraction and regex generation
        self.data_extr_ = cfg.get("data_extr_", {}).copy()
        self.regex_extr_ = cfg.get("regex_extr_", {}).copy()
        
        with open(cfg['prompt_file'], "r", encoding="utf-8") as f:
            prompts_data = json.load(f) 
            
            self.data_extr_['prompt'] = prompts_data.get(self.data_extr_['prompt'])
            self.regex_extr_['prompt'] = prompts_data.get(self.regex_extr_['prompt'])

        # Mapeamento de 'reasoning' e 'temperature' (código mantido)
        if self.regex_extr_["reasoning"] <= 0:
                self.regex_extr_["reasoning"] = "minimal"
        elif self.regex_extr_["reasoning"] <= 0.33:
                self.regex_extr_["reasoning"] = "low"
        elif self.regex_extr_["reasoning"] <= 0.66:
                self.regex_extr_["reasoning"] = "medium"
        else:
                self.regex_extr_["reasoning"] = "high"
            
        if self.data_extr_["reasoning"] <= 0:
                self.data_extr_["reasoning"] = "minimal"
        elif self.data_extr_["reasoning"] <= 0.33:
                self.data_extr_["reasoning"] = "low"
        elif self.data_extr_["reasoning"] <= 0.66:
                self.data_extr_["reasoning"] = "medium"
        else:
                self.data_extr_["reasoning"] = "high"

        if self.regex_extr_["temperature"] <=0:
             self.regex_extr_["temperature"] = "low"
        elif self.regex_extr_["temperature"] <= 0.5:
             self.regex_extr_["temperature"] = "medium"
        else:
             self.regex_extr_["temperature"] = "high"
        
        if self.data_extr_["temperature"] <=0:
             self.data_extr_["temperature"] = "low"
        elif self.data_extr_["temperature"] <= 0.5:
             self.data_extr_["temperature"] = "medium"
        else:
             self.data_extr_["temperature"] = "high"

    def _build_prompt(self, task: dict) -> str:

        if task["task"] == "data":
            try:
                with open(self.data_extr_['prompt']['prompt'], "r", encoding="utf-8") as f:
                    prompt_content = f.read()
            except FileNotFoundError:
                raise FileNotFoundError(f"Prompt file not found: {self.data_extr_['prompt']['prompt']}")
            except Exception as e:
                raise Exception(f"Error reading prompt file: {e}")

            return prompt_content.format(
                schema=self.campos_a_extrair,
                text=self.text_to_analyze
            )
        elif task["task"] == "regex":
            try:
                with open(self.regex_extr_['prompt']['prompt'], "r", encoding="utf-8") as f:
                    prompt_content = f.read()
            except FileNotFoundError:
                raise FileNotFoundError(f"Prompt file not found: {self.regex_extr_['prompt']['prompt']}")
            except Exception as e:
                raise Exception(f"Error reading prompt file: {e}")
            
            # --- Inclusão da Blacklist no Prompt ---
            failed_regex_str = "\n".join(f"- {r}" for r in self.failed_regexes) if self.failed_regexes else "Nenhuma regex falhou anteriormente para este campo."
            
            return prompt_content.format(
                schema = self.campos_a_extrair,
                text=self.text_to_analyze,
                failed_regexes=failed_regex_str # Novo parâmetro
            )
        else:
            raise ValueError(f"Unknown task type: {task['task']}")

    # Timeout de 20 segundos para todas as chamadas
    LLM_TIMEOUT = 20.0 

    async def generate_regex_json(self) -> dict:
        """Chama o LLM usando o cliente nativo da OpenAI para gerar a lista JSON."""
        
        prompt = self._build_prompt({"task": "regex"})
        start_time = time.perf_counter()
        
        messages = [
            {
                "role": "user",
                "content": prompt
            }
        ]
        
        try:
            # --- Adiciona asyncio.wait_for para Timeout ---
            response = await asyncio.wait_for(
                self.client.chat.completions.create(
                    model=self.model_name,
                    messages=messages,
                    response_format={"type": "json_object"},
                    temperature=1,
                    reasoning_effort=self.regex_extr_["reasoning"],
                    verbosity= self.regex_extr_["temperature"]
                ),
                timeout=self.LLM_TIMEOUT
            )
            # --- Fim do Timeout ---
            
            end_time = time.perf_counter()
            duration = end_time - start_time
            
            json_output_str = response.choices[0].message.content
            json_output = json.loads(json_output_str)
            
            return {
                "duration": duration,
                "json_response": json_output,
                "model_name": self.model_name,
                "prompt_used": prompt
            }
        
        except asyncio.TimeoutError:
             return {"error": f"LLM Timeout Error (model {self.model_name}): A chamada excedeu o limite de {self.LLM_TIMEOUT}s e foi cancelada.", "model_name": self.model_name}
        except OpenAIError as e:
             return {"error": f"OpenAI API Error (model {self.model_name}): {e}", "model_name": self.model_name}
        except json.JSONDecodeError as e:
             return {"error": f"Falha ao decodificar JSON do modelo {self.model_name}: {e}. Output raw: {json_output_str}", "model_name": self.model_name}
        except Exception as e:
            return {"error": f"Unexpected Error (model {self.model_name}): {e}", "model_name": self.model_name}

    async def extract_data_json(self) -> dict:
        """Chama o LLM usando o cliente nativo da OpenAI para extrair os dados diretamente em JSON."""
        prompt = self._build_prompt({"task": "data"})
        start_time = time.perf_counter()
        
        messages = [
            {
                "role": "user",
                "content": prompt
            }
        ]
               
        try:
            # --- Adiciona asyncio.wait_for para Timeout ---
            response = await asyncio.wait_for(
                self.client.chat.completions.create(
                    model=self.model_name,
                    messages=messages,
                    response_format={"type": "json_object"},
                    temperature=1,
                    reasoning_effort=self.data_extr_["reasoning"],
                    verbosity= self.data_extr_["temperature"]
                ),
                timeout=self.LLM_TIMEOUT
            )
            # --- Fim do Timeout ---
            
            end_time = time.perf_counter()
            duration = end_time - start_time
            
            json_output_str = response.choices[0].message.content
            json_output = json.loads(json_output_str)
            
            return {
                "duration": duration,
                "json_response": json_output,
                "model_name": self.model_name,
                "prompt_used": prompt
            }
        
        except asyncio.TimeoutError:
             return {"error": f"LLM Timeout Error (model {self.model_name}): A chamada excedeu o limite de {self.LLM_TIMEOUT}s e foi cancelada.", "model_name": self.model_name}
        except OpenAIError as e:
             return {"error": f"OpenAI API Error (model {self.model_name}): {e}", "model_name": self.model_name}
        except json.JSONDecodeError as e:
             return {"error": f"Falha ao decodificar JSON do modelo {self.model_name}: {e}. Output raw: {json_output_str}", "model_name": self.model_name}
        except Exception as e:
            return {"error": f"Unexpected Error (model {self.model_name}): {e}", "model_name": self.model_name}