_                                                                                              
                                                                            .---.          _______                 
_________   _...._               _________   _...._            __.....__     |   |          \  ___ `'.              
\        |.'      '-.            \        |.'      '-.     .-''         '.   |   |           ' |--.\  \             
 \        .'```'.    '.           \        .'```'.    '.  /     .-''"'-.  `. |   |           | |    \  '            
  \      |       \     \   __      \      |       \     \/     /________\   \|   |    __     | |     |  '    __     
   |     |        |    |.:--.'.     |     |        |    ||                  ||   | .:--.'.   | |     |  | .:--.'.   
   |      \      /    ./ |   \ |    |      \      /    . \    .-------------'|   |/ |   \ |  | |     ' .'/ |   \ |  
   |     |\`'-.-'   .' `" __ | |    |     |\`'-.-'   .'   \    '-.____...---.|   |`" __ | |  | |___.' /' `" __ | |  
   |     | '-....-'`    .'.''| |    |     | '-....-'`      `.             .' |   | .'.''| | /_______.'/   .'.''| |  
  .'     '.            / /   | |_  .'     '.                 `''-...... -'   '---'/ /   | |_\_______|/   / /   | |_ 
'-----------'          \ \._,\ '/'-----------'                                    \ \._,\ '/             \ \._,\ '/ 
                        `--'  `"                                                   `--'  `"               `--'  `"  

# Papelada - Desafio Enter AI Fellowship

Este repositório contém a minha solução para o "Take Home Project" da Enter AI Fellowship. O objetivo central era criar uma solução de extração de dados de PDFs que fosse rápida (menos de 10 segundos por requisição), barata (minimizando custos de LLM), e precisa (acima de 80%), sem conhecer os *labels* ou *schemas* de extração antecipadamente.

## A Solução: Abordagem Híbrida "Smart"

Para atender aos requisitos conflitantes de velocidade, custo e precisão, desenvolvi uma arquitetura híbrida que "aprende" com o tempo. A estratégia principal é evitar chamadas ao LLM sempre que possível, tratando-as como um último recurso para aprendizado, e não como a principal ferramenta de extração.

O núcleo da solução está no `extractor.py`, que opera no `"smart" mode` (definido no `config.json`). O fluxo de processamento para cada documento é o seguinte:

1.  **Carregar Memória:** O sistema primeiro carrega um arquivo de "memória" (`data/memory.json`). Este arquivo armazena regras de regex validadas que foram geradas em execuções anteriores para um determinado `label` de documento.
2.  **Aplicar Regras Conhecidas:** O `extractor.py` tenta preencher o *schema* de extração usando as regras de regex em memória. Esta etapa é quase instantânea e tem custo zero (sem LLM).
3.  **Identificar Lacunas:** O sistema verifica quais campos ainda estão como `null`.
4.  **LLM (Extração de Dados):** **Apenas para os campos faltantes**, o sistema faz uma primeira chamada ao LLM (`llm.py` usando o prompt `pt_regex_short_prompt_trust`). O objetivo é apenas obter os *dados* que faltam.
5.  **LLM (Geração de Regex):** Se os dados foram encontrados com confiança, o sistema faz uma *segunda* chamada ao LLM (`llm.py` usando o prompt `pt_data_long_prompt_trust_reasoning`). O objetivo desta chamada é gerar uma regra de regex robusta e generalista que *poderia* ter encontrado esse dado.
6.  **Validar e Salvar:** O `extractor.py` valida a nova regex contra o texto do documento. Se a regex conseguir extrair o valor esperado, ela é considerada válida e é salva no `data/memory.json`, sendo adicionada ao conjunto de regras daquele `label`.

Este sistema "acumula conhecimento" a cada execução. Na primeira vez que vê um `label` "carteira_oab", ele depende do LLM. Na segunda vez, ele já usa as regex aprendidas, tornando o processo mais rápido e barato.

## Arquitetura do Projeto

A solução é dividida nos seguintes componentes principais:

* `main.py`: O ponto de entrada da aplicação. Responsável por:
    * Interpretar argumentos de linha de comando (`--extraction_schema`, `--pdf_path`, `--config`).
    * Orquestrar o pipeline: carregar o `config.json`, carregar e processar os PDFs, e iniciar o `run`.
    * Salvar os resultados finais em `results/`.
* `pdf_pipeline.py`: Contém todas as funções utilitárias para lidar com os PDFs.
    * `extract`: Extrai o texto bruto do PDF usando `pdfplumber`.
    * `clean`: Limpa o texto (remove espaços extras, normaliza quebras de linha).
    * `normalize`: Normaliza o texto (converte para minúsculas, remove acentos, etc.) com base nas opções do `config.json`.
* `extractor.py`: O "cérebro" da solução.
    * Gerencia o `memory.json` (carregamento e salvamento).
    * Aplica regras de regex conhecidas (`_apply`).
    * Orquestra as chamadas ao `LLMExtractor` para preencher lacunas e aprender novas regras.
    * Valida as novas regex antes de salvá-las.
* `llm.py`: A camada de abstração para a API da OpenAI (`gpt-5-mini`).
    * Constrói os prompts dinamicamente usando os arquivos em `prompts/`.
    * Executa as chamadas assíncronas para extração de dados (`extract_data_json`) e geração de regex (`generate_regex_json`).
* `config.json`: Arquivo central de configuração. Define o modo de operação, caminhos de arquivos (memória, prompts) e os parâmetros para as chamadas de LLM.

## Como Utilizar

1.  **Instalação:**
    ```bash
    pip install -r requirements.txt
    ```

2.  **Configuração:**
    * Renomeie `.env.txt` para `.env`.
    * Adicione sua chave da OpenAI ao arquivo `.env`:
        ```
        OPENAI_API_KEY="sk-..."
        ```
    * *(Nota: O projeto está configurado para usar o `gpt-5-mini`, conforme a restrição do desafio)*

3.  **Execução:**
    O script `main.py` é o ponto de entrada. Ele requer um *schema* de extração e um *caminho para os PDFs*.

    ```bash
    python main.py --extraction_schema [CAMINHO_PARA_SCHEMA.json] --pdf_path [CAMINHO_PARA_PDFS_OU_PASTA]
    ```

    **Exemplo de Comando:**
    ```bash
    # (Supondo que 'extraction_guide.json' seja um arquivo que você criou
    #  seguindo o formato de 'results/teste.json', mas para guiar a extração)
    
    python main.py -e "extraction_guide.json" -p "caminho/para/meus_pdfs/" -c "config.json"
    ```

4.  **Resultados:**
    Os resultados da extração serão salvos em um arquivo JSON dentro da pasta `results/`, conforme definido no `config.json` (ex: `results/teste.json`).

## Respondendo aos Desafios do Projeto

* **Reduzir chamadas ao LLM e Minimizar Custo:**
    Conseguido através do sistema de memória (`memory.json`). O LLM só é chamado para campos desconhecidos em *labels* novos. O custo tende a zero à medida que o sistema aprende.
* **Manter 80%+ de Acurácia:**
    Conseguido através da validação de regex. Uma regex só é salva na memória se for comprovadamente capaz de extrair o valor correto. O prompt `pt_data_long_prompt_trust_reasoning` é instruído a criar regex generalistas e robustas, não literais, para lidar com variações.
* **Responder em <10s:**
    O "caminho feliz" (onde todas as regras estão em memória) é extremamente rápido, pois depende apenas de `re.search`. O "caminho frio" (primeira execução) está sujeito à latência do LLM, mas é um custo pago apenas uma vez por *label*.
* **Lidar com Variabilidade de Layout:**
    O prompt de geração de regex (`pt_data_long_prompt_trust_reasoning`) é especificamente focado em encontrar âncoras de texto (rótulos) e padrões generalistas, em vez de depender de posições fixas, permitindo que a extração funcione mesmo que o layout mude.