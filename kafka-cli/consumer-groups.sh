# Examples for kafka-console-producer command usages.

kafka-consumer-groups --bootstrap-server localhost:9092 --list

kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group <group_name>


# Resetting groups

kafka-consumer-groups --bootstrap-server localhost:9092 --topic first_topic --group group2 --reset-offsets --to-earliest --execute


kafka-coumer-groups --bootstrap-server localhost:9092 --all-topics --group group2 --reset-offsets --shift-by -2 --execute


