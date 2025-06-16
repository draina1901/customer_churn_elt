import psycopg2
import csv



def ingestionFunc(**kwargs):
    # Connect using psycopg2 directly
    conn = psycopg2.connect(
        host="host.docker.internal",
        database="cust_churn_stg",
        user="myuser",
        password="password123",
        port=6543
    )

    print("Conection Testing123")
    print(conn)
    cursor = conn.cursor()
    #I am deleting and uploading whole data again because there wont be new data coming in.
    # but in real project, we can do incremental load
    cursor.execute("DELETE FROM customers;")
    print("Old data deleted.")

    rows_ingested = 0

    # Open your CSV
    # I have removed some fields from the columns to test the transformation logic from the csv manually
    with open('customer_churn_data.csv', 'r') as f:
        reader = csv.reader(f)
        next(reader)  # Skip header row

        for row in reader:
            cursor.execute("""
                INSERT INTO customers (
                    CustomerID, Age, Gender, Tenure, MonthlyCharges, 
                    ContractType, InternetService, TotalCharges, 
                    TechSupport, Churn
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, row)
            rows_ingested +=1

    # Commit and close
    conn.commit()
    cursor.close()
    conn.close()

    print(f"{rows_ingested} rows inserted into Postgres.")
    kwargs['ti'].xcom_push(key='rows_ingested', value=rows_ingested)
    return rows_ingested