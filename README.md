# Kafka course practices

---



## Pre-requisites
- Docker

## Setting up

1. To start Kafka broker and Zookeeper execute the following command in the repo root folder 

```
docker compose up -d
```

### Useful commands

1. Use Kafka CLI on broker container
```
docker exec -it broker-local /bin/bash
```

2. List Kafka topics
```
kafka-topics --bootstrap-server localhost:9092 --list
    
```

