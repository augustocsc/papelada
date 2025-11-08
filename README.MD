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
Papelada - Desafio Enter AI Fellowship
Este repositório contém a minha solução para o "Take Home Project" da Enter AI Fellowship. O objetivo central era criar uma solução de extração de dados de PDFs que fosse rápida (menos de 10 segundos por requisição), barata (minimizando custos de LLM), e precisa (acima de 80%), sem conhecer os labels ou schemas de extração antecipadamente.

Atualizações
Novo: API de Extração! O projeto agora pode ser executado como um servidor FastAPI de alta performance. Esta API gerencia o estado (memória, config) na inicialização, lida com processamento de lotes concorrentes e expõe a lógica do orquestrador via HTTP.

Orquestrador Inteligente ("Modo Pro"): A lógica de execução foi otimizada com um agendador dinâmico de 3 filas. Ele agora analisa o lote e o estado da memória para separar trabalhos "Warm" (paralelos), "Cold Orphans" (paralelos com aprendizado) e "Cold Groups" (sequenciais de Professor/Aluno) para maximizar o throughput e o aprendizado.

Geração de Regex em Segundo Plano: O sistema desacopla a extração de dados da fase de aprendizado. A API retorna os dados extraídos quase instantaneamente, enquanto as chamadas de LLM para criar e validar regras são agendadas de forma não-bloqueante.

A Solução: Abordagem Híbrida "Smart"
Para atender aos requisitos conflitantes de velocidade, custo e precisão, desenvolvi uma arquitetura híbrida que "aprende" com o tempo. A estratégia principal é evitar chamadas ao LLM sempre que possível, tratando-as como um último recurso para aprendizado, e não como a principal ferramenta de extração.

O núcleo da solução é um orquestrador (orchestrator.py) que opera em três modos (standard, smart, pro) definidos no config.json. O fluxo de processamento é o seguinte:

Carregar Memória: O sistema primeiro carrega um arquivo de "memória" (data/memory.json). Este arquivo armazena regras de regex validadas que foram geradas em execuções anteriores.

Pré-Análise e Agendamento (Modo Pro): O orquestrador analisa todo o lote de entrada e o compara com a memória. Ele então categoriza cada trabalho:

Warm Start (Regras Existem): Estes são colocados na fila paralela rápida.

Cold Start (Órfãos): Arquivos únicos no lote. Vão para uma fila paralela de aprendizado.

Cold Start (Grupos): Arquivos idênticos que precisam aprender. Vão para la fila sequencial "Professor/Aluno".

Aplicar Regras Conhecidas: O extractor.py tenta preencher o schema usando as regras de regex em memória. Esta etapa é quase instantânea e tem custo zero.

Identificar Lacunas e Executar "Professor" (Cold Start): O sistema identifica os campos null. Para grupos "Cold Start", ele executa um arquivo ("o professor") e espera.

LLM (Extração de Dados): O "professor" (ou qualquer arquivo "cold") faz uma chamada ao LLM (llm.py) para obter os dados que faltam.

LLM (Geração de Regex): Em segundo plano (asyncio.create_task), o sistema agenda uma segunda chamada ao LLM para gerar uma regra de regex robusta que poderia ter encontrado esse dado.

Executar "Alunos": Assim que o "professor" termina, todos os "alunos" (arquivos idênticos) são executados em paralelo, beneficiando-se imediatamente das regras recém-aprendidas (ou em processo de aprendizado).

Validar e Salvar: A tarefa em segundo plano valida a nova regex. Se for válida, um asyncio.Lock protege a memória compartilhada enquanto a nova regra é salva em data/memory.json.

Este sistema "acumula conhecimento" a cada execução. Na primeira vez que vê um label "carteira_oab", ele depende do LLM. Na segunda vez, ele já usa as regex aprendidas, tornando o processo mais rápido e barato.

Arquitetura do Projeto
A solução é dividida nos seguintes componentes principais:

api_main.py (NOVO): O ponto de entrada do servidor FastAPI. Gerencia o estado da aplicação (carregamento de memória e cliente OpenAI na inicialização), lida com uploads de arquivos e expõe o orquestrador através de um endpoint /extract/ seguro e assíncrono.

main.py (LEGACY): O ponto de entrada original da aplicação para execução via CLI. Responsável por interpretar argumentos de linha de comando e iniciar o orchestrator.run.

orchestrator.py: O "cérebro" principal. Implementa a lógica de agendamento de 3 filas (Warm, Cold Orphan, Cold Group) e coordena o Extractor.

extractor.py: O "trabalhador". Gerencia o memory.json (leitura), aplica regras de regex conhecidas (_apply) e orquestra as chamadas ao LLMExtractor para preencher lacunas e agendar o aprendizado de novas regras.

llm.py: A camada de abstração para a API da OpenAI. Constrói os prompts dinamicamente e executa as chamadas assíncronas para extração e geração de regex.

pipeline.py: (Anteriormente pdf_pipeline.py) Contém todas as funções utilitárias para lidar com os PDFs (extração com pdfplumber, limpeza e normalização).

utils.py: Funções auxiliares para carregar e salvar JSONs.

config.json: Arquivo central de configuração. Define o modo de operação (standard, smart, pro), caminhos de arquivos e parâmetros de LLM.

Como Utilizar
A aplicação pode ser executada de duas formas: como um servidor API (recomendado) ou como um script CLI.

Modo 1: Execução via API (Servidor)
Este é o modo de execução principal, permitindo o processamento de lotes via HTTP.

Instalação (API): Adicione as novas dependências de servidor ao seu requirements.txt e instale-as:

fastapi
uvicorn[standard]
python-multipart
Bash

pip install -r requirements.txt
Configuração (API):

Crie um arquivo .env na raiz do projeto.

Adicione sua chave da OpenAI:

OPENAI_API_KEY="sk-..."
Adicione uma chave de API secreta para proteger seu endpoint:

PAPELADA_API_KEY="seu-token-secreto-aqui-123"
Execução (API): Inicie o servidor Uvicorn a partir da raiz do projeto:

Bash

uvicorn api_main:app --host 0.0.0.0 --port 8000 --reload
O servidor estará disponível em http://localhost:8000.

Como Usar (API): Envie uma requisição POST para o endpoint /extract/ usando multipart/form-data.

Header: X-API-Key: "seu-token-secreto-aqui-123"

Body (FormData):

extraction_schema: O seu ficheiro .json que descreve o lote (ex: results/teste.json).

pdf_files: Um ou mais ficheiros .pdf (ex: oab_1.pdf, oab_2.pdf, ...).

Exemplo de Comando curl:

Bash

curl -X POST "http://127.0.0.1:8000/extract/" \
-H "X-API-Key: seu-token-secreto-aqui-123" \
-F "extraction_schema=@results/teste.json" \
-F "pdf_files=@caminho/para/oab_1.pdf" \
-F "pdf_files=@caminho/para/oab_2.pdf" \
-F "pdf_files=@caminho/para/tela_sistema_1.pdf"
Modo 2: Execução via CLI (Script)
Este é o modo original para testes locais.

Instalação (CLI):

Bash

pip install -r requirements.txt
Configuração (CLI):

Configure seu .env com a OPENAI_API_KEY.

Execução (CLI): O script main.py requer um schema de extração e um caminho para os PDFs.

Bash

python main.py --extraction_schema [CAMINHO_PARA_SCHEMA.json] --pdf_path [CAMINHO_PARA_PDFS_OU_PASTA]
Exemplo de Comando:

Bash

python main.py -e "results/teste.json" -p "caminho/para/meus_pdfs/"
Resultados: Os resultados da extração (de ambos os modos) serão salvos em um arquivo JSON dentro da pasta results/, conforme definido no config.json (ex: results/teste.json).

Respondendo aos Desafios do Projeto
Reduzir chamadas ao LLM e Minimizar Custo: Conseguido através do sistema de memória (memory.json) e do agendamento "Warm Start", que pula o aprendizado se as regras já existem.

Manter 80%+ de Acurácia: Conseguido através da validação de regex e da blacklist com "motivo da falha", que ensina o LLM a não repetir erros.

Responder em <10s: Conseguido através da execução em asyncio e da separação da geração de regex (lenta) em tarefas de fundo não-bloqueantes. O "caminho feliz" (Warm Start) é quase instantâneo.

Lidar com Variabilidade de Layout: Conseguido através da "Assinatura de Trabalho" (label + field_set). O orquestrador agrupa trabalhos idênticos, permitindo que variações de schema (como B3 vs B1) sejam tratadas como trabalhos completamente diferentes, cada um aprendendo suas próprias regras.

Comentários Finais e Lições Aprendidas
Fiquei muito surpreso com o desafio para o Fellowship. De fato, como é dito no enunciado, OCR virou commodity e isso faz novos produtos surgirem a partir disso. Como Filipe comentou no QA, esse problema poderia ser resolvido apenas passando os dados e o comando para um LLM. De fato, o GPT-5 mini sozinho funciona melhor do que todas as tentativas que fiz para gerar uma expressão regular confiável e replicável em outros documentos.

Infelizmente meu "extra mile" não vai aparecer nos commits do projeto, porque fui muleque e gastei muito tempo brincando com a LLM. Só que a LLM errada. Para não correr o risco de gastar crédito à toa, comecei o projeto utilizando o Gemini, um modelo compatível com o do projeto. Depois de alguns ajustes ele estava me respondendo super bem e me senti confiante em avançar com o projeto.

Levantei toda a estrutura, escrevi alguns scripts para limpeza e normalização dos dados (o que acabou não sendo tão necessário). Troquei uma ideia com Gemini e Chatgpt pedindo para que eles pensassem com carinho em uma solução e ao perceber que eles não conseguiriam sugerir nada melhor do que eu pensei, comecei a migração.

Na verdade tentei manter um sistema que suportasse mais de uma família de LLM. Utilizando o LLMLite tentei criar um sistema agnóstico ao modelo, porém já estava com o tempo curto e os prompts que eu havia validado estavam com uma performance péssima. Peguei o código e pedi para o Gemini reescrever utilizando a API da OpenAI e mais surpresas. A performance do gpt para gerar código era péssima.

Depois de ajustar os dois prompts principais (para extração dos dados e criação da regex), estava praticamente sem tempo para polir a aplicação corretamente. Então, não, esse não é o melhor projeto da minha vida, mas aprendi e me diverti muito. Por mais que a interface não tenha ficado legal, eu sei que a performance pode melhorar muito só de estruturar o fluxo de execução.

Ouso dizer que para um caso de uso real essa aplicação pode realmente ser eficiente, desde que se abra algumas brechas nos requisitos como:

Remanejar a ordem de execução: já que desde o começo pensei no projeto linearmente. Poder reordenar a ordem para processar primeiro documentos com mais campos que se repetem e processar labels em paralelo ajudaria na velocidade.

Melhorar o pipeline de geração de regex: essa parte é a mais instável e que pode prejudicar o meu resultado, pois para economizar tokens tentei uma solução mais direta, não gastando muito tempo na geração.

Continuarei trabalhando no projeto, pois achei um desafio muito legal. Então se você é um avaliador, não me odeie por ter outros commits pós a data :D