# Examples for kafka-console-consumer command usages.

kafka-console-consumer --bootstrap-server localhost:9092 --topic <topic_name> [--from-beginning]


kafka-console-consumer --bootstrap-server localhost:9092 --topic first_topic --from-beginning \
--formatter kafka.tools.DefaultMessageFormatter --property print.timestamp=true --property print.key=true --property print.value=true


kafka-console-consumer --bootstrap-server localhost:9092 --topic first_topic --group group2