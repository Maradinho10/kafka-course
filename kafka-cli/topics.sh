# Examples for kafka-topics command usages.


kafka-topics --bootstrap-server localhost:9092 --list

kafka-topics --bootstrap-server localhost:9092 --list

kafka-topics --bootstrap-server localhost:9092 --create --topic <topic_name> [--partitions <num_partitions> --replication-factor <rep_factor>]

kafka-topics --bootstrap-server localhost:9092 --describe --topic <topic_name>

kafka-topics --bootstrap-server localhost:9092 --delete --topic <topic_name>
