import pandas as pd

#Aquí colocas la dirección de donde se haya descargado el archivo .xlsx
file_path = 'C:\\Users\\Andrés\\Downloads\\archive\\KPMG_VI_New_raw_data_update_final.xlsx'
xls = pd.ExcelFile(file_path)

transactions_df = pd.read_excel(xls, sheet_name='Transactions', header=1)

#Aquí es la dirección donde va a guardar el archivo .csv
transactions_df.to_csv('.\\db_neo4j\\import\\KPMG_VI_New_raw_data_update_final.csv', index=False)
