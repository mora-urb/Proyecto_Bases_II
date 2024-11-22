from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import numpy as np

load_dotenv()

def create_tables(conn):
    """Crear las tablas en PostgreSQL si no existen"""
    with conn.cursor() as cur:
        # Crear tablas
        cur.execute("""
            CREATE TABLE IF NOT EXISTS total_spent_per_customer (
                customer_id BIGINT PRIMARY KEY,
                total_spent DOUBLE PRECISION
            )
        """)
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS product_purchase_count (
                product_id BIGINT PRIMARY KEY,
                purchase_count INTEGER
            )
        """)
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS average_spend_per_customer (
                customer_id BIGINT PRIMARY KEY,
                average_spend DOUBLE PRECISION,
                transaction_count INTEGER
            )
        """)
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS transaction_count_per_customer (
                customer_id BIGINT PRIMARY KEY,
                transaction_count INTEGER
            )
        """)
        
        conn.commit()
        print("✓ Tablas creadas exitosamente")

def convert_numpy_int64(value):
    """Convertir numpy.int64 a int Python nativo"""
    if isinstance(value, np.int64):
        return int(value)
    return value

def spark_to_postgres(df, table_name, conn):
    """Convertir DataFrame de Spark e insertar en PostgreSQL"""
    # Convertir Spark DataFrame a Pandas DataFrame
    pandas_df = df.toPandas()
    
    # Convertir valores numpy.int64 a int Python nativo
    tuples = [tuple(convert_numpy_int64(x) for x in row) for row in pandas_df.values]
    
    # Crear string de placeholders para SQL
    cols = ','.join(list(df.columns))
    placeholders = ','.join(['%s'] * len(df.columns))
    
    # Preparar query
    query = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"
    
    # Ejecutar insert
    with conn.cursor() as cur:
        try:
            cur.executemany(query, tuples)
            conn.commit()
            print(f"✓ Datos guardados en {table_name}")
        except Exception as e:
            conn.rollback()
            print(f"Error guardando en {table_name}: {str(e)}")
            print(f"Primer registro como ejemplo: {tuples[0] if tuples else 'No hay datos'}")
            raise

def main():
    try:
        print("Intentando conexión a PostgreSQL...")
        # Conectar a PostgreSQL
        conn = psycopg2.connect(
            dbname=os.getenv('POSTGRES_DB'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD'),
            host="localhost",
            port="5433"  # Cambiado a 5433
        )
        print("✓ Conexión establecida exitosamente")
        
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        
        # Crear tablas
        create_tables(conn)
        
        # Importar los DataFrames ya procesados
        from Consultas_Spark import (
            total_spent_per_customer,
            products_most_bought,
            average_spend_per_customer,
            purchase_frequency_per_customer
        )
        
        # Guardar cada DataFrame
        dataframes = {
            'total_spent_per_customer': total_spent_per_customer,
            'product_purchase_count': products_most_bought,
            'average_spend_per_customer': average_spend_per_customer,
            'transaction_count_per_customer': purchase_frequency_per_customer
        }
        
        for table_name, df in dataframes.items():
            print(f"\nProcesando {table_name}...")
            spark_to_postgres(df, table_name, conn)
            
        print("\n✓ Todos los datos fueron guardados exitosamente")
            
    except Exception as e:
        print(f"Error en la ejecución: {str(e)}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()