import pandas as pd
import psycopg2
import hashlib
from sqlalchemy import create_engine, text
from datetime import datetime
import numpy as np

def hash_id(val):
    return hashlib.sha256(str(val).encode()).hexdigest()


def transformFunc(**kwargs):
    ti = kwargs['ti']
    rows_ingested = ti.xcom_pull(task_ids='data_ingestion', key='rows_ingested')

    # Read from staging using psycopg2
    stg_conn = psycopg2.connect(
        dbname='cust_churn_stg',
        user='myuser',
        password='password123',
        host='host.docker.internal',
        port=6543
    )
    df = pd.read_sql("SELECT * FROM customers", stg_conn)
    stg_conn.close()

  

    df.replace(r'^\s*$', np.nan, regex=True, inplace=True)
    nulls_before = df.isnull().sum().sum()

    # Handle missing values
    df['age'] = pd.to_numeric(df['age'], errors='coerce').fillna(0).astype(int)
    df['tenure'] = pd.to_numeric(df['tenure'], errors='coerce').fillna(0).astype(int)
    df['monthlycharges'] = pd.to_numeric(df['monthlycharges'], errors='coerce').fillna(0.0)
    df['totalcharges'] = pd.to_numeric(df['totalcharges'], errors='coerce').fillna(0.0)

    df['gender'] = df['gender'].fillna('Unknown')
    df['contracttype'] = df['contracttype'].fillna('Unknown')
    df['internetservice'] = df['internetservice'].fillna('Unknown')
    df['techsupport'] = df['techsupport'].fillna('No')
    df['churn'] = df['churn'].fillna('No')

    nulls_after = df.isnull().sum().sum()
    nulls_filled = int(nulls_before - nulls_after)

    # Anonymize PII
    pii_anonymized = df['customerid'].notnull().sum()
    df['customerid'] = df['customerid'].apply(hash_id)

    # Write to reporting DB using SQLAlchemy
    rpt_engine = create_engine('postgresql+psycopg2://myuser:password123@host.docker.internal:6543/cust_churn_rep')
    df.to_sql('customers', rpt_engine, if_exists='replace', index=False)

    errors = 0 

    # Convert numpy types to Python native types
    def to_py(x):
        return x.item() if hasattr(x, 'item') else x

    log_values = {
        'timestamp': datetime.now(),
        'rows_ingested': to_py(rows_ingested),
        'nulls_filled': to_py(nulls_filled),
        'pii_anonymized': to_py(pii_anonymized),
        'errors': to_py(errors)
    }

    # Log stats
    with rpt_engine.connect() as conn:
        conn.execute(
            text("""
                INSERT INTO ingestion_log (timestamp, rows_ingested, nulls_filled, pii_anonymized, errors)
                VALUES (:timestamp, :rows_ingested, :nulls_filled, :pii_anonymized, :errors)
            """), log_values
        )

    print("Transformation complete and ingestion stats logged.")