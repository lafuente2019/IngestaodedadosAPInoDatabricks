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
🌐 GitHub: [github.com/valterlafuente](https://github.com/valterlafuente)
