# Image de base avec Python
FROM python:3.9-slim

# Installer Java pour PySpark
RUN apt-get update && apt-get install -y default-jre && apt-get clean

# Installer PySpark
RUN pip install --no-cache-dir pyspark

# Copier les fichiers du projet
WORKDIR /app
COPY transform/ /app/transform/
COPY requirements.txt /app/

# Installer les dépendances Python
RUN pip install --no-cache-dir -r requirements.txt

# Définir le point d'entrée
CMD ["python", "transform/data_pipeline.py"]
