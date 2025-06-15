import pandas as pd
import psycopg2
import hashlib
from sqlalchemy import create_engine

def hash_id(val):
    return hashlib.sha256(str(val).encode()).hexdigest()


def transformFunc():
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

    # Handle missing values
    df['totalcharges'] = pd.to_numeric(df['totalcharges'], errors='coerce').fillna(0)
    df['tenure'] = df['tenure'].fillna(0)
    df['gender'] = df['gender'].fillna('Unknown')
    df['contracttype'] = df['contracttype'].fillna('Unknown')
    df['techsupport'] = df['techsupport'].fillna('Unknown')

    # Anonymize PII
    df['customerid'] = df['customerid'].apply(hash_id)

    # Write to reporting DB using SQLAlchemy
    rpt_engine = create_engine('postgresql+psycopg2://myuser:password123@host.docker.internal:6543/cust_churn_rep')
    df.to_sql('customers', rpt_engine, if_exists='replace', index=False)

    print("Transformation complete.")


