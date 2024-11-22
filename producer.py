from kafka import KafkaProducer
import json
import sqlite3
import time
import random

# Fonction pour convertir une base SQLite en une liste de dictionnaires
def sqlite_to_list(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Extraire les données
    cursor.execute("SELECT * FROM transactions")  # Table SQLite
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    
    # Convertir en liste de dictionnaires
    data = [dict(zip(columns, row)) for row in rows]
    conn.close()
    return data

# Charger les données JSON
with open("data/social_media_data.json", "r") as f:
    json_data = json.load(f)

# Charger les données du fichier texte et les convertir en JSON
def parse_text_file_to_json(file_path):
    with open(file_path, "r") as file:
        lines = [line.strip() for line in file if line.strip()]
        headers = [header.strip() for header in lines[0].split("|")]  # Extraire les en-têtes depuis la 1ère ligne
        data = []
        for line in lines[1:]:
            values = [value.strip() for value in line.split("|")]  # Extraire les valeurs pour chaque ligne
            entry = dict(zip(headers, values))  # Créer un dictionnaire avec en-têtes comme clés
            data.append(entry)
        return data

text_data = parse_text_file_to_json("data/logs.txt")  # Conversion en JSON

# Charger les données SQLite
sqlite_data = sqlite_to_list("data/transactions_clients.sqlite")

# Fusionner toutes les sources dans une structure indexée
data_sources = {
    "social_media": json_data,
    "text_topic": text_data,
    "sqlite_topic": sqlite_data
}

# Configurer le producteur Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8') if isinstance(v, dict) else v.encode('utf-8')
)

# Liste des topics Kafka
topics = list(data_sources.keys())

print("Démarrage de l'envoi aléatoire de messages vers Kafka...")

# Boucle pour envoyer des messages de manière aléatoire
try:
    while True:
        # Choisir un topic aléatoire
        selected_topic = random.choice(topics)
        # Choisir un message de cette source
        if data_sources[selected_topic]:  # Vérifier qu'il y a des données à envoyer
            message = data_sources[selected_topic].pop(0)  # Prendre et retirer le premier élément
            producer.send(selected_topic, message)
            print(f"Envoyé ({selected_topic}): {message}")
        else:
            print(f"Plus de données pour {selected_topic}.")
            topics.remove(selected_topic)  # Retirer la source vide
            if not topics:
                print("Toutes les données ont été envoyées.")
                break
        
        # Pause pour simuler un flux temps réel
        time.sleep(0.5)
finally:
    producer.close()
    print("Le producteur Kafka a été fermé.")
