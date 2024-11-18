import pandas as pd
from py2neo import Graph
from dotenv import load_dotenv
import os

load_dotenv()

neo4j_url = os.getenv('NEO4J_URL')
neo4j_user = os.getenv('NEO4J_USER')
neo4j_password = os.getenv('NEO4J_PASSWORD')

file_path = '.\\db_neo4j\\import\\KPMG_VI_New_raw_data_update_final.csv'
transactions_df = pd.read_csv(file_path)

transactions_df['standard_cost'] = transactions_df['standard_cost'].fillna(0)

graph = Graph(neo4j_url, auth=(neo4j_user, neo4j_password))


def load_customers():
    for index, row in transactions_df.iterrows():
        query = f"""
        MERGE (c:Customer {{customer_id: {row['customer_id']}}})
        """
        graph.run(query)

def load_products():
    for index, row in transactions_df.iterrows():
        query = f"""
        MERGE (p:Product {{product_id: {row['product_id']}, brand: '{row['brand']}', product_line: '{row['product_line']}'}})
        """
        graph.run(query)

def load_transactions():
    for index, row in transactions_df.iterrows():
        online_order_value = False if pd.isna(row['online_order']) else row['online_order']
        
        
        query = f"""
        MATCH (c:Customer {{customer_id: {row['customer_id']}}})
        MATCH (p:Product {{product_id: {row['product_id']}}})
        MERGE (c)-[:BOUGHT {{
            transaction_id: {row['transaction_id']}, 
            transaction_date: '{row['transaction_date']}',
            online_order: {online_order_value}, 
            order_status: '{row['order_status']}',
            list_price: {row['list_price']},
            standard_cost: {row['standard_cost']}
        }}]->(p)
        """
        graph.run(query)

def Carga_Completa():    
    load_customers()
    load_products()
    load_transactions()
        
    print("Datos cargados exitosamente en Neo4j.")

