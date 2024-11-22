from pyspark.sql import SparkSession

# Crée une session Spark avec les URI adaptées pour DataLake (source) et DataLake_Clean (cible)
spark = SparkSession.builder \
    .appName("MongoDB_Duplicate_Removal") \
    .config("spark.mongodb.input.uri", "mongodb+srv://Louis:Louis@datalake.h1n2r.mongodb.net/DataLake") \
    .config("spark.mongodb.output.uri", "mongodb+srv://Louis:Louis@datalake.h1n2r.mongodb.net/DataLake_Clean") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .getOrCreate()

# Fonction pour charger et nettoyer chaque collection# Fonction pour charger et nettoyer chaque collection en ignorant la colonne "timestamp"
def remove_duplicates(source_collection, target_collection, dedup_columns):
    # Charge la collection source de MongoDB (dans la base de données DataLake)
    df = spark.read.format("mongo").option("database", "DataLake").option("collection", source_collection).load()

    # Suppression des doublons en ignorant la colonne "timestamp"
    df_cleaned = df.dropDuplicates(dedup_columns)

    # Sauvegarde dans la collection cible (dans la base de données DataLake_Clean)
    df_cleaned.write.format("mongo").option("database", "DataLake_Clean").option("collection", target_collection).mode("overwrite").save()


# Liste des collections à nettoyer et leur collection cible avec colonnes pour suppression des doublons
collections = [
    ("logs", "logs_clean", ["IP_Address", "Date","Time","Method","URL","HTTP_Version","Status_Code","Response_size"]),  # Remplacez par les colonnes pertinentes pour chaque collection
    ("transactions", "transactions_clean", ["transaction_id"]),
    ("social_media", "social_media_clean", ["post_id"])
]

# Application de la suppression des doublons pour chaque collection
for source, target, dedup_columns in collections:
    remove_duplicates(source, target, dedup_columns)

# Arrêter la session Spark
spark.stop()

