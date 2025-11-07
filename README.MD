```_                                                                                              
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
```
# Papelada - Desafio Enter AI Fellowship

Este repositório contém a minha solução para o "Take Home Project" da Enter AI Fellowship. O objetivo central era criar uma solução de extração de dados de PDFs que fosse rápida (menos de 10 segundos por requisição), barata (minimizando custos de LLM), e precisa (acima de 80%), sem conhecer os *labels* ou *schemas* de extração antecipadamente.

## Atualizações

> A arquitetura **"Smart"** ficou mais rápida! Adicionei um sistema assíncrono de **Geração de Regex em Segundo Plano** que desacopla a extração de dados da fase de aprendizado. Agora, a aplicação retorna os dados extraídos quase instantaneamente, enquanto as custosas chamadas de LLM para criação e validação de novas regras de Regex são agendadas para rodar de forma não-bloqueante. Além disso, a gestão de memória foi reforçada com um sistema de **`asyncio.Lock`**, garantindo que as regras aprendidas sejam salvas de forma concorrente e segura em `memory.json` por múltiplos processos, eliminando a perda de conhecimento e aumentando a robustez do aprendizado incremental.


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
    * Crie um arquivo para `.env`.
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

## Comentários Finais e Lições Aprendidas

Fiquei muito surpreso com o desafio para o Fellowship. De fato, como é dito no enunciado, OCR virou commodity e isso faz novos produtos surgirem a partir disso. Como Filipe comentou no QA, esse problema poderia ser resolvido apenas passando os dados e o comando para um LLM. De fato, o GPT-5 mini sozinho funciona melhor do que todas as tentativas que fiz para gerar uma expressão regular confiável e replicável em outros documentos.

Infelizmente meu "extra mile" não vai aparecer nos commits do projeto, porque fui muleque e gastei muito tempo brincando com a LLM. Só que a LLM errada. Para não correr o risco de gastar crédito à toa, comecei o projeto utilizando o Gemini, um modelo compatível com o do projeto. Depois de alguns ajustes ele estava me respondendo super bem e me senti confiante em avançar com o projeto.

Levantei toda a estrutura, escrevi alguns scripts para limpeza e normalização dos dados (o que acabou não sendo tão necessário). Troquei uma ideia com Gemini e Chatgpt pedindo para que eles pensassem com carinho em uma solução e ao perceber que eles não conseguiriam sugerir nada melhor do que eu pensei, comecei a migração.

Na verdade tentei manter um sistema que suportasse mais de uma família de LLM. Utilizando o LLMLite tentei criar um sistema agnóstico ao modelo, porém já estava com o tempo curto e os prompts que eu havia validado estavam com uma performance péssima. Peguei o código e pedi para o Gemini reescrever utilizando a API da OpenAI e mais surpresas. A performance do gpt para gerar código era péssima.

Depois de ajustar os dois prompts principais (para extração dos dados e criação da regex), estava praticamente sem tempo para polir a aplicação corretamente. Então, não, esse não é o melhor projeto da minha vida, mas aprendi e me diverti muito. Por mais que a interface não tenha ficado legal, eu sei que a performance pode melhorar muito só de estruturar o fluxo de execução.

Ouso dizer que para um caso de uso real essa aplicação pode realmente ser eficiente, desde que se abra algumas brechas nos requisitos como:

* Remanejar a ordem de execução: já que desde o começo pensei no projeto linearmente. Poder reordenar a ordem para processar primeiro documentos com mais campos que se repetem e processar labels em paralelo ajudaria na velocidade.

* Melhorar o pipeline de geração de regex: essa parte é a mais instável e que pode prejudicar o meu resultado, pois para economizar tokens tentei uma solução mais direta, não gastando muito tempo na geração.


Continuarei trabalhando no projeto, pois achei um desafio muito legal. Então se você é um avaliador, não me odeie por ter outros commits pós a data :D
