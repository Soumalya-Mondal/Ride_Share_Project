# 🚖 Taxi Lookup & Ride Share Database Setup

This project sets up PostgreSQL tables for **Taxi Lookup Zones** and **Ride Share** data, and loads large-scale datasets efficiently using GPU-accelerated data processing.

---

## 📘 Overview

The project automates:
- Creation of two PostgreSQL tables:
  - **taxi_zone_lookup**
  - **ride_share_data**
- Efficient data loading into PostgreSQL using **cuDF** (GPU DataFrames) and **psycopg2**.

The **Ride Share** dataset is large (~12 GB, `.parquet` format), requiring GPU acceleration for faster loading and transformation.

---

## ⚙️ Technologies Used

| Technology | Purpose |
|-------------|----------|
| **PostgreSQL** | Database to store Taxi Lookup and Ride Share data |
| **Python** | Automation and data handling |
| **cuDF** | GPU-based DataFrame library for handling large `.parquet` files |
| **psycopg2** | PostgreSQL database connection and operations |
| **python-dotenv** | Load database credentials securely from `.env` file |
| **uv** | Fast Python package manager and virtual environment manager |

---

## 🧩 Project Structure



```
.
└── support/
    ├── taxi_zone_lookup_table_create.py  # Creates Taxi Lookup Zone table
    └── rideshare_data_table_create.py    # Creates Ride Share table
    └── taxi_zone_lookup_data_entry.py    # Inserts data into Taxi Lookup Zone
    └── rideshare_data_data_entry.py      # Inserts data into Ride Share
├── main.py                               # Main Script
├── .env                                  # Stores DB credentials 
├── README.md                             # Project documentation
├── .python-version                       # Specifies the Python version used in development
├── pyproject.toml                        # Project metadata and dependency definitions for uv
├── uv.lock                               # Lock file with exact dependency versions
└── Data_Archive/
    ├── taxi_zone_lookup.csv              # Lookup data (example)
    └── rideshare_data.parquet            # Large Ride Share dataset (~12GB)
```

---

## 🚀 How to Run

### 🧱 Step 1: Install Dependencies
```bash
uv sync
```

### 2️⃣ Step 2: Execute Script
```bash
uv run main.py
```

---

## ⚡ Performance Note

The **Ride Share** `.parquet` file (~12 GB) is processed using **cuDF** for GPU-accelerated reading and transformation before inserting into PostgreSQL.  
This drastically reduces load time compared to CPU-only approaches.

---

## 🧠 Requirements

- Python 3.9+
- PostgreSQL 13+
- NVIDIA GPU with CUDA support
- uv package manager
- Python Libraries:
  ```bash
  uv add cudf-cu12 psycopg2-binary python-dotenv
  ```

---

## 🗂️ Database Configuration

Store your PostgreSQL credentials in a `.env` file located at the project root:

```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=your_db
DB_USERNAME=username
DB_PASSWORD=yourpassword
```

Each script automatically loads these credentials using **python-dotenv**.

Example connection snippet used in all scripts:

---

## 🌐 Data Source

The Ride Share dataset is publicly available on Kaggle:  
🔗 [NYC Rideshare Raw Data – Kaggle](https://www.kaggle.com/datasets/aaronweymouth/nyc-rideshare-raw-data)

---

## 📄 License

This project is licensed under the **MIT License**.