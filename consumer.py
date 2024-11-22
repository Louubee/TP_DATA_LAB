from kafka import KafkaConsumer
from pymongo import MongoClient
import json

# Connexion à MongoDB Atlas
mongo_client = MongoClient("mongodb+srv://Louis:Louis@datalake.h1n2r.mongodb.net/?retryWrites=true&w=majority&appName=DataLake")
db = mongo_client['DataLake']  # Base de données
social_media_collection = db['social_media']  # Collection pour les données sociales
logs_collection = db['logs']  # Collection pour les logs
transactions_collection = db['transactions']  # Collection pour les transactions SQLite

print("Connexion à Kafka...")
consumer = KafkaConsumer(
    'social_media', 'text_topic', 'sqlite_topic',  # Consommer depuis les trois topics
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='consumer_group',
    value_deserializer=lambda v: v.decode('utf-8')  # Désérialisation en chaîne brute
)
print("Connexion à Kafka réussie. En attente de messages...")

# Traitement des messages Kafka
for message in consumer:
    print(f"Message reçu : {message.value}")  # Affiche le message brut reçu

    try:
        # Essayer de charger le message comme JSON
        document = json.loads(message.value)
    except json.JSONDecodeError:
        # Si ce n'est pas du JSON, garder le message tel quel
        document = message.value

    # Rediriger le message vers la bonne collection
    if isinstance(document, str):  # Message texte brut (text_topic)
        log_document = {
            "log": document,
            "source": "kafka",
            "timestamp": message.timestamp  # Timestamp Kafka
        }
        try:
            logs_collection.insert_one(log_document)
            print(f"Inséré dans MongoDB Atlas (logs) : {log_document}")
        except Exception as e:
            print(f"Erreur lors de l'insertion (logs) : {e}")
    
    elif isinstance(document, dict):  # Message JSON
        document["source"] = "kafka"
        document["timestamp"] = message.timestamp  # Ajouter le timestamp Kafka
        
        # Détection du type de document pour redirection
        if 'post_id' in document:  # Message social_media
            try:
                social_media_collection.insert_one(document)
                print(f"Inséré dans MongoDB Atlas (social_media) : {document}")
            except Exception as e:
                print(f"Erreur lors de l'insertion (social_media) : {e}")
        
        elif 'transaction_id' in document:  # Message SQLite (transactions)
            try:
                transactions_collection.insert_one(document)
                print(f"Inséré dans MongoDB Atlas (transactions) : {document}")
            except Exception as e:
                print(f"Erreur lors de l'insertion (transactions) : {e}")
        
        elif 'IP_Address' in document and 'Date' in document and 'Time' in document:  # Message logs (text_topic)
            try:
                logs_collection.insert_one(document)
                print(f"Inséré dans MongoDB Atlas (logs) : {document}")
            except Exception as e:
                print(f"Erreur lors de l'insertion (logs) : {e}")
        
        else:
            print(f"Document JSON inconnu, non inséré : {document}")
    else:
        print(f"Message non pris en charge : {message.value}")
