# Data Engineering Programming - Trabalho Final

**Professor:** Marcelo Barbosa Pinto

**Instituição:** FIAP - MBA em Engenharia de Dados

---

## 1. Objetivo do Projeto

Este projeto implementa um pipeline de dados robusto utilizando **PySpark**, fundamentado em arquitetura orientada a objetos (POO) e padrões de **injeção de dependências**.

O foco principal é o processamento de grandes volumes de dados de **pedidos e pagamentos**, com o objetivo de identificar inconsistências transacionais específicas do ano de **2025**.

---

## 2. Escopo de Negócio

O pipeline foi desenvolvido para gerar um relatório de pedidos que atendam simultaneamente aos seguintes requisitos:

* **Filtro Temporal:** Apenas registros do ano de **2025**
* **Status de Pagamento:** Somente pagamentos recusados (`status = false`)
* **Análise de Fraude:** Apenas pedidos classificados como legítimos (`fraude = false`)
* **Saída de Dados:** Relatório exportado em formato **Parquet**, contendo:

  * ID do Pedido
  * UF
  * Forma de Pagamento
  * Valor Total
  * Data
* **Ordenação:** Organizado por `UF`, `Forma de Pagamento` e `Data de Criação`

---

## 3. Arquitetura e Critérios Técnicos

O projeto segue boas práticas utilizadas em pipelines profissionais de Engenharia de Dados.

* **Schemas Explícitos**
  Implementação de `StructType` para todos os DataFrames, garantindo **tipagem forte e melhor performance** (sem uso de `inferSchema`).

* **Orientação a Objetos (POO)**
  Encapsulamento da lógica em classes especializadas:

  * Config
  * Spark
  * I/O
  * Business Logic
  * Orchestration

* **Injeção de Dependências**
  O arquivo `main.py` atua como **Aggregation Root**, responsável por instanciar as dependências e injetá-las no orquestrador.

* **Logging e Resiliência**
  Uso do pacote `logging` para rastreabilidade e blocos `try/catch` para tratamento de exceções.

* **Testes Unitários**
  Validação das transformações utilizando o framework **pytest**.

* **Empacotamento Profissional**
  Estrutura preparada para distribuição com:

  * `pyproject.toml`
  * `requirements.txt`
  * `MANIFEST.in`

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
```

---

# 5. Instruções de Execução

Todas as etapas necessárias para rodar o pipeline estão descritas abaixo.

---

## 5.1 Pré-requisitos

O ambiente deve possuir:

* **Python:** versão **3.10 ou superior**
* **Java:** **JDK 8 ou 11** (necessário para execução do Spark)
* **Apache Spark:** versão **3.x** configurada nas variáveis de ambiente

---

## 5.2 Configuração do Ambiente

### Clonar o repositório

```bash
git clone https://github.com/ggava/Data-Engineering-Programming.git
cd Data-Engineering-Programming
```

### Criar ambiente virtual

```bash
python -m venv venv
```

Ativação do ambiente:

**Windows**

```bash
.\venv\Scripts\activate
```

**Linux / macOS**

```bash
source venv/bin/activate
```

### Instalar dependências

```bash
pip install -r requirements.txt
```

---

## 5.3 Execução do Pipeline

O arquivo **`main.py`** orquestra todo o fluxo de processamento.

Para executar o pipeline completo:

```bash
python main.py
```

### Processamento realizado

O pipeline irá:

1. Ler os arquivos de **pedidos e pagamentos**
2. Aplicar os filtros:

   * Pedidos do ano **2025**
   * Pagamentos **recusados**
   * Pedidos **não fraudulentos**
3. Gerar um relatório final em **formato Parquet** na pasta de saída configurada.

---

## 5.4 Execução dos Testes

Para validar as regras de negócio e a integridade das transformações:

```bash
python -m pytest src/tests/test_logic.py
```
