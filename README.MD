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

Este repositório contém a minha solução para o "Take Home Project" da Enter AI Fellowship. O objetivo central era criar uma solução de extração de dados de PDFs que fosse rápida (menos de 10 segundos por requisição), barata (minimizando custos de LLM), e precisa (acima de 80%), sem conhecer os labels ou schemas de extração antecipadamente.

**Última versão antes do MEIO DIA no dia 07/11: https://github.com/augustocsc/papelada/tree/0285f93b4e945060d72d77b3a741f1d9f1aee090**
## Atualizações

### **v2.1: Estabilidade e Experiência do Usuário (UX)**

A última rodada de atualizações focou em corrigir falhas críticas de estabilidade e elevar o nível da interface para o estilo **Neobrutalista**.

  * **Fluxo Assíncrono Real (`WebSocket`):** A extração síncrona de dados agora é totalmente separada da geração de regras e da avaliação (processos lentos).
      * O *frontend* recebe atualizações de progresso **arquivo por arquivo** (real-time).
      * A tela de resultados é exibida **imediatamente** após a extração de todos os dados, sem esperar pelo LLM ou testes.
  * **Correção Crítica de Memória (Fix):** Corrigido o bug de referência de objeto (`AttributeError` e `memory.copy()`) que impedia que as regras de regex válidas fossem salvas no arquivo `data/memory.json`. O aprendizado e a persistência de regras agora funcionam corretamente.
  * **Melhoria na Lógica Smart/Pro:** A lógica de agrupamento foi corrigida no `orchestrator.py` para garantir que o **Modo Smart** e o **Modo Pro** agrupem documentos pela `label`, forçando o fluxo sequencial de "Professor/Aluno" e maximizando o aprendizado em lote.
  * **UX/UI Neobrutalista Ousada:**
      * Interface reconstruída no estilo **Neobrutalista** (bordas grossas, sombras duras, cores neutras e contrastantes).
      * **Título Imponente:** O logo textual "Papelada" foi estilizado com a fonte Bebas Neue, grande e com sombra forte, e o subtítulo foi alterado para **"Devagar também é Pressa"**.
      * **Ajuda Contextual:** Adicionado um botão discreto **`(?)`** aos modos de execução e um modal de "Ajuda" para explicar os conceitos de LLM híbrido e os modos de operação.
  * **Segurança da Sessão:** O botão "Encerrar Sessão" agora **limpa as chaves de API do navegador** e envia uma requisição `DELETE` para o *backend* para **apagar o arquivo de memória** (`data/memory.json`), garantindo a privacidade e a limpeza da sessão.

### **v2.0 (Anterior): Arquitetura e Performance**

  - **Orquestrador Inteligente ("Modo Pro"):** A lógica de execução foi otimizada com um agendador dinâmico de 3 filas. Ele agora analisa o lote e o estado da memória para separar trabalhos "Warm" (paralelos), "Cold Orphans" (paralelos com aprendizado) e "Cold Groups" (sequenciais de Professor/Aluno) para maximizar o throughput e o aprendizado.

  - **Geração de Regex em Segundo Plano:** O sistema desacopla a extração de dados da fase de aprendizado. A API retorna os dados extraídos quase instantaneamente, enquanto as chamadas de LLM para criar e validar regras são agendadas de forma não-bloqueante.

## A Solução: Abordagem Híbrida "Smart"

Para atender aos requisitos conflitantes de velocidade, custo e precisão, desenvolvi uma arquitetura híbrida que "aprende" com o tempo. A estratégia principal é evitar chamadas ao LLM sempre que possível, tratando-as como um último recurso para aprendizado, e não como a principal ferramenta de extração.

O núcleo da solução é um orquestrador (`orchestrator.py`) que opera em três modos (standard, smart, pro) definidos no `config.json`. O fluxo de processamento é o seguinte:

1.  **Carregar Memória:** O sistema primeiro carrega um arquivo de "memória" (`data/memory.json`). Este arquivo armazena regras de regex validadas que foram geradas em execuções anteriores.
2.  **Pré-Análise e Agendamento (Modo Pro/Smart):** O orquestrador analisa todo o lote de entrada e o compara com a memória. Ele então agrupa os trabalhos primariamente pela `label` (ex: `carteira_oab`) e os categoriza.
3.  **Filas de Execução:** Os trabalhos são distribuídos em filas:
      * **Warm Start (Paralelo):** Arquivos que têm todas as regras necessárias na memória.
      * **Grupos de Ensino (Sequencial):** Grupos de trabalhos `Cold` que precisam aprender. O primeiro documento utiliza o LLM e tenta salvar a regra **imediatamente**.
4.  **Extração de Dados (Síncrona):** O `extractor.py` tenta preencher o schema. Se as regras falharem, ele faz uma chamada LLM (GPT-5) para obter o dado faltante.
5.  **Notificação Imediata:** Assim que a extração síncrona de um arquivo termina, o *backend* envia uma notificação `progress` via WebSocket. Quando todos terminam, o *frontend* avança para a tela de resultados.
6.  **Geração de Regex (Assíncrona):** Se o LLM de dados foi usado e obteve sucesso, uma tarefa em segundo plano (não-bloqueante) é criada. Esta tarefa faz uma **segunda chamada LLM** para gerar uma regra de regex robusta.
7.  **Validação e Salvamento da Memória:** A tarefa de fundo valida a nova regex comparando-a com o dado extraído (usando normalização para ser tolerante a espaços/quebras de linha). Se for válida, a regra é salva no `data/memory.json`.

Este sistema "acumula conhecimento" a cada execução. Na segunda vez que vê um documento similar, ele já usa as regex aprendidas, tornando o processo mais rápido e barato.

## Arquitetura do Projeto

A solução é dividida nos seguintes componentes principais:

  - **`api_main.py` (NOVO/WebSocket):** O servidor FastAPI. Gerencia o estado e protege os endpoints HTTP. O endpoint `/ws/extract_live/` gerencia o fluxo de extração em tempo real e orquestra a execução das tarefas assíncronas.
  - **`orchestrator.py`:** O "cérebro" principal. Implementa a lógica de agendamento e coordena o Extractor.
  - **`extractor.py`:** O "trabalhador". Gerencia o `memory.json`, aplica regras de regex conhecidas e orquestra as chamadas ao `LLMExtractor` para preencher lacunas e agendar o aprendizado de novas regras.
  - **`llm.py`:** A camada de abstração para a API da OpenAI. Constrói os prompts dinamicamente e executa as chamadas assíncronas para extração e geração de regex.
  - **`frontend/index.html`:** O painel de controle Neobrutalista que lida com a autenticação (chaves API), a seleção de modo, o display de progresso real-time e a visualização dos resultados em um modal limpo.

## Como Utilizar

A aplicação deve ser executada como um servidor API.

#### 1\. Instalação:

Instale as dependências necessárias:

```bash
pip install -r requirements.txt
```

#### 2\. Configuração:

Crie um arquivo `.env` na raiz do projeto e defina as chaves de API. O frontend irá tentar usar a chave `PAPELADA_API_KEY` por padrão para autenticação.

```
PAPELADA_API_KEY=15d3b1c6-ece7-4483-913d-e16c73eb7320"
OPENAI_API_KEY="sk-..." # Opcional: Se omitido, o usuário deve inserir no frontend.
```

#### 3\. Execução:

Inicie o servidor Uvicorn a partir da raiz do projeto (usando o comando `uvicorn api_main:app --host 0.0.0.0 --port 8000 --reload`).

#### 4\. Uso (Frontend):

1.  Abra `frontend/index.html` no seu navegador.
2.  Na tela de configuração, se o `PAPELADA_API_KEY` estiver salvo, clique em **Iniciar Nova Sessão**.
3.  Selecione os ficheiros (`schema.json` e `pdfs`).
4.  Selecione o **Modo de Execução** (Smart recomendado para aprendizado).
5.  Clique em **Iniciar Extração**.
6.  O progresso será exibido em tempo real, e a tela de resultados aparecerá assim que a extração síncrona for concluída.

<!-- end list -->

```
```
