import asyncio
import time
import json
from openai import OpenAIError

class LLMExtractor:
    def __init__(self, cfg: dict, campos_a_extrair: list, text_to_analyze: str, client=None): 
        self.model_name = cfg['model_name']

        if client is None:
            raise ValueError("Client cannot be None. Please provide an AsyncOpenAI client instance.")
        self.client = client
        
        self.campos_a_extrair = campos_a_extrair
        self.text_to_analyze = text_to_analyze
        
        self.data_extr_ = cfg.get("data_extr_", {}).copy()
        self.regex_extr_ = cfg.get("regex_extr_", {}).copy()
        
        with open(cfg['prompt_file'], "r", encoding="utf-8") as f:
            prompts_data = json.load(f) 
            self.data_extr_['prompt'] = prompts_data.get(self.data_extr_['prompt'])
            self.regex_extr_['prompt'] = prompts_data.get(self.regex_extr_['prompt'])

        def map_value_to_level(value):
            if value <= 0: return "minimal" if 'reasoning' in locals() else "low"
            elif value <= 0.33: return "low"
            elif value <= 0.66: return "medium"
            else: return "high"
        
        for config_key in ["regex_extr_", "data_extr_"]:
            for key in ["reasoning", "temperature"]:
                config = getattr(self, config_key)
                if key == "reasoning": config[key] = map_value_to_level(config[key]) if config[key] > 0 else "minimal"
                else: config[key] = map_value_to_level(config[key])

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
            
            return prompt_content.format(
                schema = self.campos_a_extrair,
                text=self.text_to_analyze
            )
        else:
            raise ValueError(f"Unknown task type: {task['task']}")

    # --- MUDANÇA (Req 10s) ---
    # O timeout da LLM foi aumentado para um valor mais generoso (30s),
    # já que não está mais preso ao limite de 10s do orquestrador.
    # Isto dá mais tempo para a geração de regex em background.
    LLM_TIMEOUT = 30.0 

    async def generate_regex_json(self) -> dict:
        """Chama o LLM para gerar a lista JSON e INCLUI DADOS DE USO (tokens)."""
        
        prompt = self._build_prompt({"task": "regex"})
        start_time = time.perf_counter()
        
        messages = [{"role": "user", "content": prompt}]
        
        try:
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
            
            end_time = time.perf_counter()
            duration = end_time - start_time
            
            json_output_str = response.choices[0].message.content
            json_output = json.loads(json_output_str)
            usage = response.usage 

            return {
                "duration": duration,
                "json_response": json_output,
                "model_name": self.model_name,
                "prompt_used": prompt,
                "usage": {
                    "prompt_tokens": usage.prompt_tokens,
                    "completion_tokens": usage.completion_tokens,
                    "total_tokens": usage.total_tokens
                }
            }
        
        except asyncio.TimeoutError:
             return {"error": f"LLM Timeout Error (model {self.model_name})", "model_name": self.model_name}
        except OpenAIError as e:
             return {"error": f"OpenAI API Error (model {self.model_name}): {e}", "model_name": self.model_name}
        except json.JSONDecodeError as e:
             return {"error": f"Falha ao decodificar JSON (model {self.model_name})", "model_name": self.model_name}
        except Exception as e:
            return {"error": f"Unexpected Error (model {self.model_name}): {e}", "model_name": self.model_name}

    async def extract_data_json(self) -> dict:
        """Chama o LLM para extrair dados e INCLUI DADOS DE USO (tokens)."""
        prompt = self._build_prompt({"task": "data"})
        start_time = time.perf_counter()
        
        messages = [{"role": "user", "content": prompt}]
               
        try:
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
            
            end_time = time.perf_counter()
            duration = end_time - start_time
            
            json_output_str = response.choices[0].message.content
            json_output = json.loads(json_output_str)
            usage = response.usage

            return {
                "duration": duration,
                "json_response": json_output,
                "model_name": self.model_name,
                "prompt_used": prompt,
                "usage": {
                    "prompt_tokens": usage.prompt_tokens,
                    "completion_tokens": usage.completion_tokens,
                    "total_tokens": usage.total_tokens
                }
            }
        
        except asyncio.TimeoutError:
             return {"error": f"LLM Timeout Error (model {self.model_name})", "model_name": self.model_name}
        except OpenAIError as e:
             return {"error": f"OpenAI API Error (model {self.model_name}): {e}", "model_name": self.model_name}
        except json.JSONDecodeError as e:
             return {"error": f"Falha ao decodificar JSON (model {self.model_name})", "model_name": self.model_name}
        except Exception as e:
            return {"error": f"Unexpected Error (model {self.model_name}): {e}", "model_name": self.model_name}