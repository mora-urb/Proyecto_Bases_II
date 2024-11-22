import pandas as pd
from py2neo import Graph
from dotenv import load_dotenv
import os

load_dotenv()

# Obtener la ruta absoluta del directorio actual
current_dir = os.path.dirname(os.path.abspath(__file__))
project_dir = os.path.dirname(current_dir)
file_path = os.path.join(project_dir, 'db_neo4j', 'import', 'KPMG_VI_New_raw_data_update_final.csv')

neo4j_url = os.getenv('NEO4J_URL')
neo4j_user = os.getenv('NEO4J_USER')
neo4j_password = os.getenv('NEO4J_PASSWORD')

print(f"Leyendo archivo desde: {file_path}")
transactions_df = pd.read_csv(file_path)

# Verificación inicial
print("\nEstadísticas antes de cargar:")
print("Valores nulos en standard_cost:", transactions_df['standard_cost'].isnull().sum())
print("Estadísticas de standard_cost:\n", transactions_df['standard_cost'].describe())

graph = Graph(neo4j_url, auth=(neo4j_user, neo4j_password))

def Carga_Completa():
    # Primero, limpiar la base de datos
    print("Limpiando base de datos...")
    graph.run("MATCH (n) DETACH DELETE n")
    
    print("Iniciando carga de datos...")
    
    # Cargar clientes
    print("Cargando clientes...")
    for index, row in transactions_df.iterrows():
        query = f"""
        MERGE (c:Customer {{customer_id: {row['customer_id']}}})
        """
        graph.run(query)
    
    # Cargar productos
    print("Cargando productos...")
    for index, row in transactions_df.iterrows():
        query = f"""
        MERGE (p:Product {{
            product_id: {row['product_id']},
            brand: '{row['brand']}',
            product_line: '{row['product_line']}'
        }})
        """
        graph.run(query)
    
    # Cargar transacciones
    print("Cargando transacciones...")
    total = len(transactions_df)
    for index, row in transactions_df.iterrows():
        # Asegurar tipos de datos correctos
        standard_cost = float(row['standard_cost']) if pd.notna(row['standard_cost']) else 0.0
        list_price = float(row['list_price']) if pd.notna(row['list_price']) else 0.0
        online_order = bool(row['online_order']) if pd.notna(row['online_order']) else False
        
        query = f"""
        MATCH (c:Customer {{customer_id: {row['customer_id']}}})
        MATCH (p:Product {{product_id: {row['product_id']}}})
        MERGE (c)-[t:BOUGHT]->(p)
        SET t.transaction_id = {row['transaction_id']},
            t.transaction_date = '{row['transaction_date']}',
            t.online_order = {str(online_order).lower()},
            t.order_status = '{row['order_status']}',
            t.list_price = {list_price},
            t.standard_cost = {standard_cost}
        """
        try:
            graph.run(query)
            if (index + 1) % 1000 == 0:
                print(f"Progreso: {index + 1}/{total} transacciones")
        except Exception as e:
            print(f"Error en transacción {row['transaction_id']}: {e}")
    
    # Verificación final
    print("\nVerificando carga...")
    result = graph.run("""
    MATCH (c:Customer)-[t:BOUGHT]->(p:Product)
    RETURN COUNT(*) as total_transactions,
           COUNT(t.standard_cost) as transactions_with_cost,
           COUNT(*) - COUNT(t.standard_cost) as transactions_without_cost
    """).data()
    
    print("\nResultados finales:")
    print(f"Total transacciones: {result[0]['total_transactions']}")
    print(f"Con standard_cost: {result[0]['transactions_with_cost']}")
    print(f"Sin standard_cost: {result[0]['transactions_without_cost']}")

if __name__ == "__main__":
    Carga_Completa()