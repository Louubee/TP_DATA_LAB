# 3 terminaux ubuntu : 
## Le premier : 
Lance zookeper puis un topic kafka 

sudo /usr/share/zookeeper/bin/zkServer.sh start
sudo /usr/local/kafka/bin/kafka-server-start.sh /usr/local/kafka/config/server.properties

Créer trois topic topic :
sudo /usr/local/kafka/bin/kafka-topics.sh --create --topic social_media --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
sudo /usr/local/kafka/bin/kafka-topics.sh --create --topic txt_topic --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

sudo /usr/local/kafka/bin/kafka-topics.sh --create --topic sqlite_topic --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1


Le deuxième :
Lance le producer.py
Python3 producer.py

Le troisième :
Lance le consumer.py
Python3 consumer.py



# Annexe :
## list des topics existant :
sudo /usr/local/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092

