kafka-configs --bootstrap-server 127.0.0.1:9092 --entity-type <entity_type> --entity-name <entity_name> --describe

kafka-configs --bootstrap-server 127.0.0.1:9092 --entity-type <entity_type> \
--entity-name <entity_name> --alter --add-config <config>