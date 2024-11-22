import streamlit as st
import pandas as pd
import plotly.express as px
import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

def get_connection():
    return psycopg2.connect(
        dbname=os.getenv('POSTGRES_DB'),
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD'),
        host="localhost",
        port="5433"
    )

def load_data():
    conn = get_connection()
    
    # Cargar datos de las tablas
    total_spent = pd.read_sql_query("""
        SELECT * FROM total_spent_per_customer 
        ORDER BY total_spent DESC LIMIT 20
    """, conn)
    
    products = pd.read_sql_query("""
        SELECT * FROM product_purchase_count 
        ORDER BY purchase_count DESC LIMIT 20
    """, conn)
    
    avg_spend = pd.read_sql_query("""
        SELECT * FROM average_spend_per_customer 
        ORDER BY average_spend DESC LIMIT 20
    """, conn)
    
    conn.close()
    return total_spent, products, avg_spend

def main():
    st.title('Análisis de Transacciones de Clientes')
    
    try:
        total_spent, products, avg_spend = load_data()
        
        # 1. Gasto total por cliente (gráfico de barras)
        st.header('Top 20 Clientes por Gasto Total')
        fig1 = px.bar(
            total_spent,
            x='customer_id',
            y='total_spent',
            title='Gasto Total por Cliente',
            labels={'customer_id': 'ID del Cliente', 'total_spent': 'Gasto Total ($)'}
        )
        st.plotly_chart(fig1)
        
        # 2. Productos más comprados
        st.header('Top 20 Productos más Comprados')
        fig2 = px.bar(
            products,
            x='product_id',
            y='purchase_count',
            title='Productos más Comprados',
            labels={'product_id': 'ID del Producto', 'purchase_count': 'Cantidad de Compras'}
        )
        st.plotly_chart(fig2)
        
        # 3. Gasto promedio por cliente
        st.header('Top 20 Clientes por Gasto Promedio')
        fig3 = px.bar(
            avg_spend,
            x='customer_id',
            y='average_spend',
            title='Gasto Promedio por Cliente',
            labels={'customer_id': 'ID del Cliente', 'average_spend': 'Gasto Promedio ($)'}
        )
        st.plotly_chart(fig3)
        
        # Tablas de datos
        st.header('Datos en Tablas')
        
        st.subheader('Top 20 Clientes por Gasto Total')
        st.dataframe(total_spent)
        
        st.subheader('Top 20 Productos más Comprados')
        st.dataframe(products)
        
        st.subheader('Top 20 Clientes por Gasto Promedio')
        st.dataframe(avg_spend)
        
    except Exception as e:
        st.error(f'Error cargando los datos: {str(e)}')

if __name__ == "__main__":
    main()