# SCALA
SCALA project

Dans un premier terminal lancer cette commande : bin/zookeeper-server-start.sh config/zookeeper.properties

Dans un deuxieme terminal lancer cette commande : bin/kafka-server-start.sh ./config/server.properties

Dans dans un troisième terminal lancer ces commandes : 
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic alert
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic report

Enfin la commande 'sbt run' vous permettra de lancer le component de votre choix.
