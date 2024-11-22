from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os

load_dotenv()

# Configuración de la conexión a PostgreSQL
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_DB = os.getenv('POSTGRES_DB')
POSTGRES_URL = f"jdbc:postgresql://localhost:5432/{POSTGRES_DB}"

def main():
    # Crear la sesión de Spark con el driver JDBC
    spark = SparkSession.builder \
        .appName("Save_to_PostgreSQL") \
        .config("spark.jars", "postgresql-42.7.1.jar") \
        .master("local[*]") \
        .getOrCreate()
    
    # Importar los DataFrames de Consultas_Spark.py
    from Consultas_Spark import (
        total_spent_per_customer,
        products_most_bought,
        average_spend_per_customer,
        purchase_frequency_per_customer
    )
    
    try:
        print("Guardando resultados en PostgreSQL...")
        
        # 1. Total gastado por cliente
        total_spent_per_customer.write \
            .format("jdbc") \
            .option("url", POSTGRES_URL) \
            .option("dbtable", "total_spent_per_customer") \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()
        print("✓ Tabla total_spent_per_customer guardada")
        
        # 2. Conteo de productos comprados
        products_most_bought.write \
            .format("jdbc") \
            .option("url", POSTGRES_URL) \
            .option("dbtable", "product_purchase_count") \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()
        print("✓ Tabla product_purchase_count guardada")
        
        # 3. Gasto promedio por cliente
        average_spend_per_customer.write \
            .format("jdbc") \
            .option("url", POSTGRES_URL) \
            .option("dbtable", "average_spend_per_customer") \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()
        print("✓ Tabla average_spend_per_customer guardada")
        
        # 4. Frecuencia de compra por cliente
        purchase_frequency_per_customer.write \
            .format("jdbc") \
            .option("url", POSTGRES_URL) \
            .option("dbtable", "transaction_count_per_customer") \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()
        print("✓ Tabla transaction_count_per_customer guardada")
        
        print("\nTodos los datos han sido guardados exitosamente en PostgreSQL")
        
    except Exception as e:
        print(f"Error al guardar los datos: {str(e)}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()