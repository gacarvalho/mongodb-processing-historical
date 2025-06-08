🧭 ♨️ COMPASS
---

<p align="left">
  <img src="https://img.shields.io/badge/projeto-Compass-blue?style=flat-square" alt="Projeto">
  <img src="https://img.shields.io/badge/versão-1.0.0-blue?style=flat-square" alt="Versão">
  <img src="https://img.shields.io/badge/status-deployed-green?style=flat-square" alt="Status">
  <img src="https://img.shields.io/badge/autor-Gabriel_Carvalho-lightgrey?style=flat-square" alt="Autor">
</p>

Essa aplicação faz parte do projeto **compass-deployment** que é uma solução desenvolvida no contexto do programa Data Master, promovido pela F1rst Tecnologia, com o objetivo de disponibilizar uma plataforma robusta e escalável para captura, processamento e análise de feedbacks de clientes do Banco Santander.


![<data-master-compass>](https://github.com/gacarvalho/repo-spark-delta-iceberg/blob/main/header.png?raw=true)



`📦 artefato` `iamgacarvalho/iamgacarvalho/dmc-app-silver-reviews-mongodb`

- **Versão:** `1.0.1`
- **Repositório:** [GitHub](https://github.com/gacarvalho/mongodb-processing-historical)
- **Imagem Docker:** [Docker Hub](https://hub.docker.com/repository/docker/iamgacarvalho/dmc-app-silver-reviews-mongodb/tags/1.0.1/sha256-6138a44faa031c50a8f8b7b4e75db092a8d03a62a0124b9e4414f999788e0d69)
- **Descrição:**  Coleta avaliações de clientes nos canais via base de dados interna da Instituição, realizando a ingestão a partir da camada Bronze, processando e aplicando tratamento de dados e armazenando no HDFS em formato **Parquet**.
- **Parâmetros:**


    - `$CONFIG_ENV` (`Pre`, `Pro`) → Define o ambiente: `Pre` (Pré-Produção), `Pro` (Produção).

| Componente          | Descrição                                                                             |
|---------------------|---------------------------------------------------------------------------------------|
| **Objetivo**        | Coletar, processar e armazenar avaliações de apps da insitituição em uma base interna para a camada Silver |
| **Entrada**         | Ambiente (pre/prod)                                                                   |
| **Saída**           | Dados válidos/inválidos em Parquet + métricas no Elasticsearch                        |
| **Tecnologias**     | PySpark, Elasticsearch, Parquet, SparkMeasure                                         |
| **Fluxo Principal** | 1. Coleta dos dados brutos → 2. Aplica padronização → 3. Separação → 4. Armazenamento |
| **Validações**      | Duplicatas, nulos em campos críticos, consistência de tipos                           |
| **Particionamento** | Por data referencia de carga (odate)                                                  |
| **Métricas**        | Tempo execução, memória, registros válidos/inválidos, performance Spark               |
| **Tratamento Erros**| Logs detalhados, armazenamento separado de dados inválidos                            |
| **Execução**        | `repo_trfmation_mongodb.py <env>`                                                 |
