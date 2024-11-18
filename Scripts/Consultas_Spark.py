from neo4j import GraphDatabase
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from dotenv import load_dotenv
import os

load_dotenv()

neo4j_url = os.getenv('NEO4J_URL')
neo4j_user = os.getenv('NEO4J_USER')
neo4j_password = os.getenv('NEO4J_PASSWORD')

driver = GraphDatabase.driver(neo4j_url, auth=(neo4j_user, neo4j_password))

# Función para ejecutar una consulta Cypher en Neo4j
def execute_query(query):
    with driver.session() as session:
        result = session.run(query)
        return list(result)

query = """
MATCH (c:Customer)-[t:BOUGHT]->(p:Product) 
RETURN c.customer_id, p.product_id, t.standard_cost
"""

# Ejecutar la consulta y obtener los resultados
result = execute_query(query)

# Convertir los resultados a un DataFrame de Pandas
data = []
for record in result:
    data.append({
        "Customer_ID": record["c.customer_id"],
        "Product_ID": record["p.product_id"],
        "Standard_Cost": record["t.standard_cost"]
    })

# Crear el DataFrame de Pandas
df = pd.DataFrame(data)

# Mostrar el DataFrame de Pandas
#print("Data from Neo4j as Pandas DataFrame:")
#print(df)

# Crear la sesión de Spark
spark = SparkSession.builder \
    .appName("Neo4J_Spark_Project") \
    .config("spark.master", "local[*]") \
    .getOrCreate()

# Convertir el DataFrame de Pandas a un DataFrame de PySpark
spark_df = spark.createDataFrame(df)

# Mostrar el DataFrame de PySpark
print("\nData from Pandas converted to PySpark DataFrame:")
spark_df.show()

# 1. **Gastado por Cliente** (sumar el Standard_Cost por cada cliente)
total_spent_per_customer = spark_df.groupBy("Customer_ID") \
    .sum("Standard_Cost") \
    .withColumnRenamed("sum(Standard_Cost)", "Total_Spent")

print("\nGastado por Cliente:")
total_spent_per_customer.show()

# 2. **Productos más Comprados** (contar las compras por cada producto)
products_most_bought = spark_df.groupBy("Product_ID") \
    .count() \
    .withColumnRenamed("count", "Purchase_Count")

print("\nProductos más Comprados:")
products_most_bought.show()

# 3. **Promedio por Cliente** (calcular el gasto promedio por cliente)
average_spend_per_customer = spark_df.groupBy("Customer_ID") \
    .agg({'Standard_Cost': 'sum', 'Standard_Cost': 'count'}) \
    .withColumnRenamed('sum(Standard_Cost)', 'Total_Spent') \
    .withColumnRenamed('count(Customer_ID)', 'Transaction_Count') \
    .withColumn('Average_Spend', spark_df['Total_Spent'] / spark_df['Transaction_Count'])

print("\nPromedio por Cliente:")
average_spend_per_customer.show()

# 4. **Frecuencia de Compra por Cliente** (contar cuántas transacciones realizó cada cliente)
purchase_frequency_per_customer = spark_df.groupBy("Customer_ID") \
    .count() \
    .withColumnRenamed("count", "Transaction_Count")

print("\nFrecuencia de Compra por Cliente:")
purchase_frequency_per_customer.show()
