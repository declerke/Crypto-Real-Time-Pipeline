# 🪙 CryptoStream: Real-Time CDC Pipeline & Analytics Engine

**CryptoStream** is a high-performance data engineering solution designed to capture, process, and visualize volatile cryptocurrency market data in real-time. Built on a **Change Data Capture (CDC)** architecture, the system ensures zero-loss data propagation from a live API staging area to a distributed NoSQL sink, maintaining sub-second synchronization for executive-level monitoring.



---

## 🎯 Project Goal
To build a resilient, event-driven ETL pipeline that demonstrates professional-grade handling of real-time streams using **Kafka Connect**, **Debezium**, and **Apache Cassandra**, while maintaining environment isolation through automated Linux/WSL deployment scripts.

---

## 🧬 System Architecture
The pipeline utilizes a decoupled "Source-Broker-Sink" logic:

1.  **Ingestion Layer:** Python-based poller fetches live USDT pairs from the Binance API and performs upserts into **PostgreSQL**.
2.  **CDC Layer (The Bridge):** **Debezium** monitors PostgreSQL's Write-Ahead Logs (WAL), converting row-level database updates into event streams.
3.  **Messaging Layer:** **Apache Kafka** acts as the high-throughput backbone, decoupling data ingestion from downstream storage.
4.  **NoSQL Sink:** **DataStax Cassandra Sink** consumes Kafka events and persists them into a wide-column store optimized for time-series queries.
5.  **Observability:** **Grafana** provides a live dashboard querying Cassandra directly to track price trends and pipeline latency.



---

## 🛠️ Technical Stack
| Layer | Tools | Purpose |
| :--- | :--- | :--- |
| **Ingestion** | Python 3.12, Requests | Live API polling and PostgreSQL staging |
| **Relational DB** | PostgreSQL 15+ | Primary staging with WAL enabled for CDC |
| **Stream Processing**| Kafka, Zookeeper | Event-driven messaging and high-availability |
| **CDC Engine** | Debezium, Kafka Connect | Log-based change data capture |
| **NoSQL Storage** | Apache Cassandra | Highly-scalable time-series data storage |
| **Infrastructure** | Docker, WSL2 (Ubuntu) | Containerized deployment and Linux environment |
| **Monitoring** | Grafana, Bash | Real-time dashboards and pipeline health scripts |

---

## 📊 Performance & Results
* **Low Latency:** Achieved a consistent **0s - 6s** end-to-end latency from API ingestion to Cassandra persistence.
* **Data Throughput:** Successfully handling real-time updates for **650+ USDT symbol pairs** every 30 seconds.
* **High Availability:** Integrated a **Self-Healing Poller** and **Auto-Recovery Connectors** that automatically restart on failure.
* **Environment Isolation:** Automated setup using **Python Virtual Environments (venv)** to bypass PEP 668 system-wide restrictions on modern Linux.

---

## 📂 Project Structure
```text
crypto-real-time-pipeline/
├── connectors/             # Kafka Connect JSON configs & registration scripts
├── ingestion/              # Python poller logic & API handlers
├── logs/                   # Process IDs (PID) and pipeline logs
├── scripts/                # Setup & Verification bash utilities
├── sql/                    # PostgreSQL and Cassandra schema definitions
├── docker-compose.yml      # Multi-service container orchestration
├── requirements.txt        # Python dependencies
└── README.md
```

---

## ⚙️ Installation & Setup

### 1. Environment & Services
Ensure Docker and Python are installed on your WSL/Linux environment.
```bash
git clone [https://github.com/declerke/Crypto-Real-Time-Pipeline.git](https://github.com/declerke/Crypto-Real-Time-Pipeline.git)
cd Crypto-Real-Time-Pipeline
chmod +x scripts/*.sh connectors/*.sh
./scripts/setup.sh
```

### 2. Verify Pipeline
Run the health monitor to validate all 6 architectural layers:
```bash
./scripts/verify_pipeline.sh
```

### 3. Access Dashboards
* **Grafana:** `http://localhost:3000` (User/Pass: `admin` / `admin`)
* **Kafka Connect:** `http://localhost:8083/connectors`

---

## 🎓 Skills Demonstrated
* **Modern CDC Implementation:** Using Debezium to turn a traditional DB into a real-time event source.
* **Distributed Systems:** Orchestrating a multi-container environment with Kafka and Cassandra.
* **Infrastructure as Code (IaC):** Designing robust shell scripts for automated environment setup and dependency management.
* **Performance Tuning:** Optimizing Kafka Connect tasks and Cassandra clustering orders for sub-second latency.
