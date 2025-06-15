import psycopg2
import csv



def ingestionFunc():
    # Connect using psycopg2 directly
    conn = psycopg2.connect(
        host="host.docker.internal",
        database="cust_churn_stg",
        user="myuser",
        password="password123",
        port=6543
    )

    # Now you can use .cursor()
    print("Conection Testing123")
    print(conn)
    cursor = conn.cursor()
    #cursor.execute("SELECT * FROM customers LIMIT 5;")
    #print(cursor.fetchall())
    cursor.execute("DELETE FROM customers;")
    print("Old data deleted.")

    # Open your CSV
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

    # Commit and close
    conn.commit()
    cursor.close()
    conn.close()

    print("CSV data inserted into Postgres successfully.")