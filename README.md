
# 📈 Coleta de Dados de Ações da B3 via API Alpha Vantage (Databricks + Delta Lake)

## 🧩 Descrição do Projeto

Este projeto realiza a **extração diária de cotações de ações da B3** utilizando a API pública da [Alpha Vantage](https://www.alphavantage.co/), processa os dados com **Python/Pandas**, e grava os resultados em uma **tabela Delta** dentro do **Unity Catalog (workspace.bronze)**.

O objetivo é manter um **histórico automatizado de preços diários** (abertura, alta, baixa, fechamento e volume) das principais ações brasileiras listadas na B3, como `PETR4`, `VALE3`, `ITUB4`, etc.

---

## ⚙️ Arquitetura e Fluxo do Processo

1. **Entrada (Input):**
   - Lista de tickers das ações da B3 a serem consultadas.
   - Chave de API da Alpha Vantage (`API_KEY`).

2. **Processamento (Process):**
   - Cada ticker é consultado individualmente na API (`https://www.alphavantage.co/query`).
   - O script respeita o **limite gratuito de 25 requisições/dia**, realizando **apenas 5 chamadas por execução**.
   - Os dados retornados em JSON são convertidos em **DataFrames Pandas** e padronizados.

3. **Saída (Output):**
   - Os dados processados são gravados em uma **tabela Delta** dentro do **Unity Catalog (`workspace.bronze.cotacoes`)**.
   - Caso a tabela ainda não exista, ela é criada automaticamente.
   - Nas execuções seguintes, os dados são adicionados em modo `append`.

---

## 🧠 Estrutura Principal do Código

### 1️⃣ Função: `buscar_dados_acao()`

Busca os dados diários de um ticker da B3.

```python
def buscar_dados_acao(ticker_b3, api_key):
    """
    Função para buscar dados diários de ações da B3 usando a API da Alpha Vantage.

    Parâmetros:
    ----------
    ticker_b3 : str
        Código da ação na B3 (exemplo: 'PETR4', 'VALE3', 'ITUB4').
    api_key : str
        Chave de acesso (API Key) obtida no site da Alpha Vantage.

    Retorno:
    -------
    pandas.DataFrame ou None
        Retorna um DataFrame com os dados diários da ação se a consulta for bem-sucedida.
        Retorna None caso ocorra erro ou não haja dados disponíveis.
    """
```

#### 🔍 Principais passos:
- Adiciona o sufixo `.SA` ao ticker para indicar que é da B3.
- Monta a URL e parâmetros da requisição.
- Realiza a chamada à API e converte o resultado JSON em DataFrame.
- Normaliza as colunas para:
  - `Abertura`
  - `Alta`
  - `Baixa`
  - `Fechamento`
  - `Volume`
- Adiciona colunas auxiliares:
  - `ticker` → código da ação original.
  - `data_ingestao` → timestamp da execução.

#### 🚦 Controle de Limite
O script contém um contador global que **impede mais de 5 chamadas diárias**, evitando atingir o limite da API gratuita.

---

### 2️⃣ Loop de Execução e Gravação no Delta Lake

```python
dfs = []
for ticker in tickets:
    df = buscar_dados_acao(ticker, API_KEY)
    if df is not None:
        print(f"✅ Ticker {ticker} coletado com sucesso!")
        dfs.append(df)
    else:
        print(f"⚠️ Falha ao coletar dados do ticker {ticker}")
```

- Faz a coleta sequencial de cada ação.
- Apenas os tickers válidos entram na lista `dfs`.

Após a coleta, os DataFrames são concatenados e gravados no Delta Lake:

```python
if dfs:
    df_final = pd.concat(dfs, ignore_index=True)
    df_spark = spark.createDataFrame(df_final)
```

---

### 3️⃣ Gravação no Unity Catalog (Delta Table)

O código verifica se a tabela `workspace.bronze.cotacoes` já existe:

- **Se não existir:** cria com `mode("overwrite")`.
- **Se existir:** adiciona os dados com `mode("append")`.

```python
tabela_existe = spark.catalog.tableExists("workspace.bronze.cotacoes")

if not tabela_existe:
    df_spark.write.format("delta").mode("overwrite").saveAsTable("workspace.bronze.cotacoes")
else:
    df_spark.write.format("delta").mode("append").saveAsTable("workspace.bronze.cotacoes")
```

---

## 🧱 Estrutura Final da Tabela

| Coluna         | Tipo         | Descrição |
|----------------|---------------|------------|
| `data`         | date          | Data da cotação |
| `Abertura`     | float         | Preço de abertura |
| `Alta`         | float         | Preço máximo do dia |
| `Baixa`        | float         | Preço mínimo do dia |
| `Fechamento`   | float         | Preço de fechamento |
| `Volume`       | float         | Volume negociado |
| `ticker`       | string        | Código da ação |
| `data_ingestao`| timestamp     | Data e hora da coleta |

---

## 🚀 Execução no Databricks

1. Configure sua **chave de API** na variável `API_KEY` crie um arquivo .env para criala.
2. Liste os tickers desejados:
   ```python
   tickets = ["PETR4", "VALE3", "ITUB4", "BBDC4", "ABEV3"]
   ```
3. Execute o notebook.
4. Após o término, valide com:
   ```python
   spark.sql("SELECT * FROM workspace.bronze.cotacoes LIMIT 5").show(truncate=False)
   ```

---

## ⚠️ Limitações e Cuidados

- A conta gratuita da **Alpha Vantage** permite **25 requisições por dia** e **5 por minuto**.  
  O script já limita automaticamente a **5 requisições diárias**.
- Caso atinja o limite, a API retornará:
  ```
  {"Note": "Thank you for using Alpha Vantage! Our standard API call frequency is 25 calls per day..."}
  ```
- É recomendado aguardar 1 minuto entre execuções ou adicionar `time.sleep(12)` entre chamadas.

---

## 📦 Dependências

- `requests`
- `pandas`
- `pyspark`
- `datetime`

Instalação:
```bash
pip install requests pandas pyspark
```

---

## 🧰 Exemplo de Saída

| data       | Abertura | Alta | Baixa | Fechamento | Volume  | ticker | data_ingestao         |
|-------------|----------|------|--------|-------------|---------|---------|-----------------------|
| 2025-11-01  | 30.25    | 31.10 | 29.90 | 30.80       | 1500000 | PETR4  | 2025-11-02 09:30:00  |


---

## 📚 Referências

- [Alpha Vantage API Documentation](https://www.alphavantage.co/documentation/)
- [Databricks Delta Lake Docs](https://docs.databricks.com/delta/)
- [Pandas Documentation](https://pandas.pydata.org/)


---


# Projeto de Ingestão de Cotações da B3 via Yahoo Finance no Databricks

## 📘 Descrição

Este projeto implementa um pipeline para **coleta, transformação e armazenamento de cotações de ações da B3 (Bolsa de Valores do Brasil)** utilizando a **API do Yahoo Finance** integrada ao **Databricks**.  

O objetivo é manter um **repositório histórico unificado** com dados de múltiplos tickers, permitindo análises de mercado, automação de relatórios e integração com camadas analíticas posteriores (Silver e Gold).

---

## 🧱 Arquitetura da Solução

```text
Yahoo Finance API
        ↓
Python (yfinance)
        ↓
Pandas → PySpark (Databricks)
        ↓
Delta Lake (Unity Catalog - Bronze Layer)
```

### Etapas Principais
1. **Extração:** Requisições automáticas ao Yahoo Finance via `yfinance`;
2. **Transformação:** Padronização dos campos e normalização dos dados em Pandas;
3. **Carga:** Conversão para Spark DataFrame e gravação incremental em Delta Lake;
4. **Governança:** Estruturação sob o schema `workspace.bronze` no Unity Catalog.

---

## ⚙️ Dependências

Execute o comando abaixo no início do notebook Databricks:

```bash
%pip install yfinance
```

---

## 🧩 Tecnologias Utilizadas

| Componente | Tecnologia |
|-------------|-------------|
| API de Dados | Yahoo Finance |
| Linguagem | Python |
| Framework | PySpark / Databricks |
| Armazenamento | Delta Lake (Unity Catalog) |
| Controle de Versão | GitHub |
| Scheduler | Databricks Jobs |

---

## 🧠 Estrutura do Código

### 1️⃣ Função de Coleta — `buscar_dados_acao()`

```python
import yfinance as yf
import pandas as pd
from datetime import datetime

def buscar_dados_acao(ticker_b3):
    ticker = f"{ticker_b3}.SA"
    print(f"Coletando dados do ticker: {ticker}...")

    df = yf.download(ticker, period="6mo", interval="1d", group_by="ticker")

    if df.empty:
        print(f"Nenhum dado encontrado para {ticker_b3}.")
        return None

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(1)

    df.reset_index(inplace=True)
    df.rename(columns={
        "Date": "data",
        "Open": "Abertura",
        "High": "Alta",
        "Low": "Baixa",
        "Close": "Fechamento",
        "Volume": "Volume"
    }, inplace=True)

    df["ticker"] = ticker_b3
    df["data_ingestao"] = datetime.now()

    return df[["data", "Abertura", "Alta", "Baixa", "Fechamento", "Volume", "ticker", "data_ingestao"]]
```

---

### 2️⃣ Coleta Consolidada de Múltiplos Tickers

```python
tickets = ["PETR4", "VALE3", "ITUB4", "BBDC4", "ABEV3"]
dfs = []

for ticker in tickets:
    df = buscar_dados_acao(ticker)
    if df is not None:
        dfs.append(df)

if not dfs:
    raise ValueError("Nenhum dado retornado pelo Yahoo Finance.")

df_final = pd.concat(dfs, ignore_index=True)
```

---

### 3️⃣ Gravação no Delta Lake

```python
df_spark = spark.createDataFrame(df_final)

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.bronze")

tabela_existe = spark.catalog.tableExists("workspace.bronze.cotacoes_yfinance")

if not tabela_existe:
    (
        df_spark.write
        .format("delta")
        .option("mergeSchema", "true")
        .mode("overwrite")
        .saveAsTable("workspace.bronze.cotacoes_yfinance")
    )
else:
    (
        df_spark.write
        .format("delta")
        .option("mergeSchema", "true")
        .mode("append")
        .saveAsTable("workspace.bronze.cotacoes_yfinance")
    )
```

---

### 4️⃣ Validação dos Dados Gravados

```python
spark.sql("SELECT * FROM workspace.bronze.cotacoes_yfinance LIMIT 10").show(truncate=False)
```

---

## 🧾 Schema Final da Tabela

| Campo | Tipo | Descrição |
|--------|------|-----------|
| `data` | timestamp | Data da cotação |
| `Abertura` | float | Preço de abertura |
| `Alta` | float | Maior preço do dia |
| `Baixa` | float | Menor preço do dia |
| `Fechamento` | float | Preço de fechamento |
| `Volume` | int | Volume negociado |
| `ticker` | string | Código da ação (ex: PETR4) |
| `data_ingestao` | timestamp | Data/hora de ingestão |

---

## 💡 Boas Práticas e Extensões

- Utilize **partitionBy("ticker")** para melhor desempenho em consultas.
- Verifique o schema atual da tabela usando `DESCRIBE TABLE`.
- Realize contagem de registros por ticker para validação dos dados.

---

## ✅ Benefícios da Solução

- Schema padronizado e flexível para múltiplos tickers  
- Compatibilidade total com Unity Catalog e Delta Lake  
- Escrita incremental e segura via `mergeSchema=True`  
- Coleta gratuita (sem necessidade de API Key)  
- Versionamento e governança integrados ao GitHub  
- Pronto para integração com camadas Silver/Gold  

---

## 🚀 Execução no Databricks

1. Clone o repositório no Databricks:
   ```bash
   git clone https://github.com/<seu-usuario>/<seu-repositorio>.git
   ```

2. Instale as dependências:
   ```bash
   %pip install yfinance
   ```

3. Configure e execute o notebook principal:
   - Ajuste a lista de tickers (`tickets = [...]`);
   - Execute as células no cluster Databricks.

4. Valide a gravação:
   ```python
   spark.sql("SELECT * FROM workspace.bronze.cotacoes_yfinance LIMIT 10").show()
   ```

---

## 👨‍💻 Autor

**Valter Lafuente Junior**  
Engenheiro de Dados — Projeto Databricks / GCP  
📧 *valterlafuentejunior@gmail.com*  
🌐 GitHub: [github.com/lafuente2019](https://github.com/lafuente2019)
