# Data Engineering Programming - Trabalho Final
 
**Professor:** Marcelo Barbosa Pinto  
**Instituição:** FIAP - MBA em Engenharia de Dados
 
---
 
## 1. Objetivo do Projeto
 
Este projeto implementa um pipeline de dados robusto utilizando **PySpark**, fundamentado em arquitetura orientada a objetos (POO) e padrões de injeção de dependências.
 
O foco principal é o processamento de grandes volumes de dados de pedidos e pagamentos para identificar inconsistências transacionais específicas do ano de **2025**.
 
---
 
## 2. Escopo de Negócio
 
O pipeline foi desenvolvido para atender a uma solicitação da alta gestão, gerando um relatório de pedidos que cumpram simultaneamente os seguintes requisitos:
 
- **Filtro Temporal:** Apenas registros do ano de **2025**
- **Status de Pagamento:** Somente pagamentos recusados (`status = false`)
- **Análise de Fraude:** Somente pedidos classificados como legítimos (`fraude = false`)
- **Saída de Dados:** Relatório exportado em formato **Parquet**, contendo:
  - ID do Pedido
  - UF
  - Forma de Pagamento
  - Valor Total
  - Data
- **Ordenação:** Organizado por **UF**, **Forma de Pagamento** e **Data de Criação**
 
---
 
## 3. Arquitetura e Critérios Técnicos
 
O projeto cumpre rigorosamente os requisitos técnicos de avaliação:
 
- **Schemas Explícitos:** Implementação de `StructType` para todos os DataFrames, garantindo tipagem forte e performance (sem `inferSchema`)
- **Orientação a Objetos (POO):** Encapsulamento de toda a lógica em classes especializadas (Config, Spark, I/O, Business Logic e Orchestration)
- **Injeção de Dependências:** O arquivo `main.py` funciona como **Aggregation Root**, instanciando as dependências e injetando-as no orquestrador
- **Logging e Resiliência:** Uso do pacote `logging` para rastreabilidade total e blocos `try/catch` para tratamento de exceções na camada de lógica
- **Testes Unitários:** Validação das transformações de dados utilizando o framework `pytest`
- **Empacotamento Profissional:** Inclusão de `pyproject.toml`, `requirements.txt` e `MANIFEST.in`
 
---
 
## 4. Estrutura do Repositório
 
```text
├── config/             # Classes de configuração centralizada
├── spark/              # Gerenciamento da SparkSession
├── io_utils/           # Classes de leitura (CSV/JSON) e escrita (Parquet)
├── business_logic/     # Core da aplicação (Transformações e Regras)
├── pipeline/           # Orquestração das etapas do fluxo
├── tests/              # Testes unitários com Pytest
├── schema/             # Definição dos StructTypes (Schemas explícitos)
├── main.py             # Ponto de entrada (Aggregation Root)
├── requirements.txt    # Listagem de dependências
├── pyproject.toml      # Configuração de build do projeto
└── MANIFEST.in         # Manifesto para distribuição