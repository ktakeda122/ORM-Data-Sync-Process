# Sakila Data Sync: MySQL to SQLite ETL

## Overview
This project implements an **ORM-only data synchronization process (ETL)** that extracts data from the operational **Sakila MySQL database**, transforms it into a **Star Schema** optimized for analytics, and loads it into a **SQLite database**.

The tool utilizes **SQLAlchemy** for database abstraction and provides a CLI for initialization, full loading, incremental updates, and data validation.

## Prerequisites
* **Python 3.8+**
* **MySQL Server** (Running the Sakila sample database)
* **SQLite3** (Built-in with Python)

## Setup Instructions

### 1. Clone the repository
```bash
git clone <your-repo-url>
cd sakila_sync
```

### 2. Install dependencies
```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install requirements
pip install sqlalchemy mysql-connector-python
```

### 3. Database Configuration
```bash
# db.py

# Update the connection string with your MySQL credentials
# Format: mysql+mysqlconnector://<user>:<password>@<host>/sakila
MYSQL_URL = "mysql+mysqlconnector://root:password@localhost/sakila"
Note: Please update the MYSQL_URL in db.py with your local MySQL credentials before running.

# SQLite path (default creates analytics.db in the current directory)
SQLITE_URL = "sqlite:///analytics.db"
```

## Usage (CLI Commands)
The tool is executed via main.py. The following commands are available:

### 1. Initialize Database
Creates the empty analytics schema (Dimensions, Facts, Bridges) and the sync_state table in SQLite. It also pre-populates the dim_date table.
```bash
python main.py init
```

### 2. Full Load
Performs a complete refresh of the analytics database.
* Action: Truncates (clears) all existing data in SQLite.
* Sync: Loads all records from MySQL.
* Reset: Resets the sync_state timestamps.
```bash
python main.py full-load
```

### 3. Incremental Load
Syncs only data that has changed since the last execution.
* Action: Checks the sync_state table for the last execution time.
* Sync: Fetches only MySQL records where last_update > last sync time.
* Update: Performs an "Upsert" (Update existing / Insert new) operation.
```bash
python main.py incremental
```

### 4. Validate Data
Checks data consistency between Source (MySQL) and Target (SQLite).
* Action: Compares row counts for all tables.
* Metric: Compares Total Revenue (Sum of amounts) to ensure financial accuracy.
```bash
python main.py validate
```


## Analytics Schema
```
erDiagram
    FACT_RENTAL ||--|| DIM_DATE : "Rented Date"
    FACT_RENTAL ||--|| DIM_FILM : "Film Key"
    FACT_RENTAL ||--|| DIM_CUSTOMER : "Customer Key"
    FACT_RENTAL ||--|| DIM_STORE : "Store Key"
    
    FACT_PAYMENT ||--|| DIM_DATE : "Payment Date"
    FACT_PAYMENT ||--|| DIM_CUSTOMER : "Customer Key"
    FACT_PAYMENT ||--|| DIM_STORE : "Store Key"

    DIM_FILM ||--|{ BRIDGE_FILM_ACTOR : has
    BRIDGE_FILM_ACTOR }|--|| DIM_ACTOR : has
    
    DIM_FILM ||--|{ BRIDGE_FILM_CATEGORY : has
    BRIDGE_FILM_CATEGORY }|--|| DIM_CATEGORY : has

    FACT_RENTAL {
        int rental_id
        int film_key
        int customer_key
        int store_key
        int date_key_rented
        int date_key_returned
        int rental_duration_days
    }
    
    FACT_PAYMENT {
        int payment_id
        float amount
        int customer_key
        int store_key
        int date_key_paid
    }
```

## Project Structure
```
sakila_sync/
├── main.py           # CLI Entry point
├── etl.py            # Core ETL logic (Full/Incremental sync functions)
├── models.py         # SQLAlchemy models (MySQL Reflection & SQLite Definition)
├── db.py             # Database connection settings (Engine/Session)
└── README.md         # Documentation
```