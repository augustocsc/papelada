import os
import json
from openai import OpenAI
from dotenv import load_dotenv
import re
import time # Importa o módulo time

# Carrega variáveis de ambiente (.env)
load_dotenv()

# Verifica se a chave da OpenAI está disponível
if not os.getenv("OPENAI_API_KEY"):
    print("⚠️ AVISO: OPENAI_API_KEY não encontrada nas variáveis de ambiente.")

client = OpenAI()

def generate_regex_from_llm(document_text, extraction_schema):
    """
    Solicita ao LLM que gere regexes para os campos baseados no texto do documento.
    """

    # Construção do Prompt
    system_prompt = """
**ROLE:**
Você é um especialista em extração de dados de documentos.

**OBJETIVO:**
Extrair informações de um texto com base em um esquema fornecido.

**INSTRUÇÕES:**
1. Analise o `DOCUMENT_TEXT` e o `EXTRACTION_SCHEMA`.
2. Para cada campo no schema, encontre o valor correspondente no texto, Ele nem sempre estará na sequência do rótulo.
3. Se um campo não estiver presente no texto, retorne `null` para ele.
4. Retorne APENAS um JSON válido com os dados extraídos e o nível de confiança: campo: ["dado", "confiança"].
5. Cuidado para não incluir texto adicional ou rótulos.
---
    """

    user_prompt = f"""
    `DOCUMENT_TEXT`:

    {document_text}

    `EXTRACTION_SCHEMA`:
    {json.dumps(extraction_schema, ensure_ascii=False)}

    ### SAÍDA ESPERADA (JSON):
    """

    # Chamada à API (usando gpt-4o-mini como proxy para o 'gpt-5 mini' mencionado no desafio, ajuste conforme necessário)
    response = client.chat.completions.create(
        model="gpt-5-mini", # Substitua pelo nome exato do modelo fornecido no desafio 
        response_format={"type": "json_object"},
        temperature=1, # Temperatura baixa para respostas mais determinísticas
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        reasoning_effort="minimal",
        verbosity="low"
    )

    # Extração do resultado
    try:
        regex_schema = json.loads(response.choices[0].message.content)
        return regex_schema
    except json.JSONDecodeError:
        print("Erro ao decodificar JSON da resposta do LLM.")
        return {}

# --- EXEMPLO DE USO ---

document_text = """JOANA D'ARC
Inscrição Seccional Subseção
101943 PR CONSELHO SECCIONAL - PARANÁ
SUPLEMENTAR
Endereço Profissional
AVENIDA PAULISTA, Nº 2300 andar Pilotis, Bela Vista
SÃO PAULO - SP
01310300
Telefone Profissional
SITUAÇÃO REGULAR"""

extraction_schema = {
    "nome": "Nome do profissional, normalmente no canto superior esquerdo da imagem",
    "inscricao": "Número de inscrição do profissional",
    "seccional": "Seccional do profissional",
    "subsecao": "Subseção à qual o profissional faz parte",
    "categoria": "Categoria, pode ser ADVOGADO, ADVOGADA, SUPLEMENTAR, ESTAGIARIO, ESTAGIARIA",
    "endereco_profissional": "Endereço do profissional",
    "telefone_profissional": "Telefone do profissional",
    "situacao": "Situação do profissional, normalmente no canto inferior direito."
}

# Medindo o tempo de execução
start_time = time.time()
regex_results = generate_regex_from_llm(document_text, extraction_schema)
end_time = time.time()

# Exibindo o resultado formatado
print(json.dumps(regex_results, indent=2, ensure_ascii=False))
print(f"\nTempo de execução para generate_regex_from_llm: {end_time - start_time:.2f} segundos")

# Teste de extração com as regexes geradas
def extract_data_with_regex(text, regex_schema):
    extracted_data = {}
    for field, pattern_info in regex_schema.items():
        # Verifica se 'pattern_info' é um dicionário e contém a chave 'regex'
        if isinstance(pattern_info, dict) and 'regex' in pattern_info and pattern_info['regex']:
            pattern = pattern_info['regex']
            try:
                # Remove o prefixo 'r' se presente, para garantir que a string seja tratada como regex
                if pattern.startswith("r'") and pattern.endswith("'"):
                    pattern = pattern[2:-1]
                elif pattern.startswith("r\"") and pattern.endswith("\""):
                    pattern = pattern[2:-1]

                match = re.search(pattern, text, re.MULTILINE | re.IGNORECASE) # Adicionado IGNORECASE para robustez
                if match:
                    # Tenta capturar o primeiro grupo, se existir
                    if match.groups():
                        extracted_data[field] = match.group(1).strip()
                    else:
                        # Se não há grupo de captura, retorna a correspondência completa
                        extracted_data[field] = match.group(0).strip()
                else:
                    extracted_data[field] = None
            except re.error as e:
                print(f"Erro na regex para o campo {field} ('{pattern}'): {e}")
                extracted_data[field] = None
        else:
            extracted_data[field] = None
    return extracted_data

# Ajuste na chamada para extract_data_with_regex para passar apenas as regexes
# O LLM retorna um dicionário onde cada chave de campo contém outro dicionário com 'regex'
# Precisamos adaptar a função extract_data_with_regex para lidar com essa estrutura
# ou pré-processar regex_results para extrair apenas as regexes.
# A função extract_data_with_regex foi ajustada para lidar com a estrutura de saída do LLM.

extracted_data = extract_data_with_regex(document_text, regex_results)
print("\nDados Extraídos:")
print(json.dumps(extracted_data, indent=2, ensure_ascii=False))
