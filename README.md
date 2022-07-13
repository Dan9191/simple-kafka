# kafka-simple-example

запускаем Zookeper
$./zookeeper-server-start.sh ../config/zookeeper.properties

Запускам брокер Kafka
$./kafka-server-start.sh ../config/server.properties

создаем топик с 3 партициями
$./kafka-topics.sh --create --topic sample-topic --partitions 3 --bootstrap-server localhost:9092

запускам консольного читателя
$ ./kafka-console-consumer.sh --topic sample-topic --bootstrap-server localhost:9092

После запуска метода main класса Producer в консольном консьумере произойдет вывод сообщений.
Если запустить  метод main класса Consumer 1 раз, то он будет читать сообщения со всех 3 партиций, если запустить.
дополнительных 2 читателей, произойдет ребалансировка, и каждый Consumer будет читать со своей партиции.
