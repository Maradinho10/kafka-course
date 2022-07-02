# Examples for kafka-console-producer command usages.

# Will open a producer and will be able to send info to the topic
kafka-console-producer --bootstrap-server localhost:9092 --topic <topic_name> [--producer-property acks=<ack_option>]

kafka-console-producer --bootstrap-server localhost:9092 --describe --topic first_topic --property parse.key=true --property key.separator=:

