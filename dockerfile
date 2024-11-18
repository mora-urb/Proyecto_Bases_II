# Usamos una imagen base de Python
FROM python:3.9-slim

# Establecemos el directorio de trabajo en el contenedor
WORKDIR /app

# Copiamos el archivo de requerimientos dentro del contenedor
COPY requirements.txt /app/requirements.txt

# Instalamos las dependencias necesarias
RUN pip install --no-cache-dir -r requirements.txt

# Copiamos el código fuente dentro del contenedor
COPY . /app

# Expón el puerto si es necesario para el contenedor de Python
EXPOSE 5000

# Comando por defecto para ejecutar el proyecto
CMD ["python", "app.py"]
