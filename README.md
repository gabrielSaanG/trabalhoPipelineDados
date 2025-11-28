# 📘 Projeto Airflow CNPJ – Guia de Instalação e Uso

Este repositório contém um pipeline completo de processamento de dados do CNPJ utilizando **Apache Airflow**, **PySpark** e execução dentro do **WSL (Ubuntu)**. O objetivo é fornecer um ambiente simples, reprodutível e automatizado para extração, limpeza, transformação e monitoramento dos dados públicos de empresas brasileiras.

---

## 🚀 Pré-requisitos

Antes de iniciar, você precisa ter instalado no Windows:

* **Windows 10 ou 11** atualizado
* **WSL2 (Windows Subsystem for Linux)**
* **Ubuntu 22.04 ou superior no WSL**
* **Docker Desktop** com integração habilitada para WSL2

---

## 🐧 1. Instalando o WSL + Ubuntu

Abra o PowerShell **como administrador** e execute:

```powershell
wsl --install -d Ubuntu
```

Após finalizar, abra o Ubuntu no menu iniciar e configure seu usuário.

Atualize o sistema:

```bash
sudo apt update && sudo apt upgrade -y
```

---

## 🐳 2. Instalando o Docker no WSL

No Windows, instale o **Docker Desktop**:

👉 [https://www.docker.com/products/docker-desktop/](https://www.docker.com/products/docker-desktop/)

Durante a instalação, habilite:

✔ "Enable WSL integration"
✔ "Use WSL 2 instead of Hyper-V"

Depois, no Ubuntu, valide:

```bash
docker --version
docker compose version
```

Se funcionar, está pronto!

---

## 🌬️ 3. Instalando o Apache Airflow (via Docker Compose)

Este projeto já inclui toda a estrutura necessária para rodar o Airflow.

No WSL, crie o diretório padrão do Airflow:

```bash
sudo mkdir -p /opt/airflow
sudo chmod -R 777 /opt/airflow
```

---

## 📁 4. Clonando o repositório e organizando os arquivos

Clone o repositório dentro do Ubuntu:

```bash
git clone https://github.com/seu_usuario/seu_repositorio.git
cd seu_repositorio
```

Agora copie a estrutura para dentro de `/opt/airflow`:

```bash
cp -R dags data docker-compose.yaml .env /opt/airflow/
```

A estrutura final deve ficar assim:

```
/opt/airflow
├── dags/
│   └── python_pipeline_cnpj.py
├── data/
│   └── cnpj_abertos/
│       ├── bronze/
│       ├── silver/
│       ├── gold/
│       └── monitoramento/
├── docker-compose.yaml
└── .env
```

---

## ▶️ 5. Subindo o Airflow

Dentro de `/opt/airflow`, execute:

```bash
docker compose up -d
```

Isso irá iniciar:

* Scheduler
* Webserver
* Worker
* Redis
* Postgres

A interface do Airflow estará em:

👉 [http://localhost:8080](http://localhost:8080)

Usuário padrão:

```
user: airflow
password: airflow
```

---

## 📝 6. Estrutura do Pipeline

O DAG principal está em:

```
dags/python_pipeline_cnpj.py
```

Ele contém as seguintes etapas:

1. **extract_bronze** – download e extração dos dados
2. **analisar_qualidade** – métricas de qualidade e estatísticas iniciais
3. **limpar_dados** – tratamento e padronização
4. **transformar_dados** – enriquecimento e geração do dataset final
5. **gerar_estatisticas** – KPIs e gráficos
6. **load_database** – envio para um banco SQLite/Parquet
7. **monitor_pipeline** – coleta de métricas de execução

---

## 📊 7. Monitoramento e Gráficos

O pipeline gera automaticamente:

* Um arquivo CSV com as métricas por task
* Gráficos de duração
* Gráficos de registros processados
* Gráficos estatísticos adicionais

Eles ficam em:

```
/opt/airflow/data/cnpj_abertos/monitoramento/
```

Inclui arquivos como:

* **monitoramento_log.csv**
* **duracao_por_task.png**
* **registros_por_task.png**
* **estatisticas_resumo.json**

---

## 🔄 8. Reiniciando o Airflow

```bash
docker compose down
```

E subir novamente:

```bash
docker compose up -d
```

---

## 🧹 9. Limpando o ambiente

Para excluir tudo (incluindo banco e logs):

```bash
sudo rm -rf /opt/airflow
```

---

## 🤝 Contribuições

Pull requests são bem-vindos! Sinta-se à vontade para abrir issues com sugestões ou correções.

---

## ⭐ Dê uma estrela!

Se este projeto ajudou você, deixe uma ⭐ no repositório!
