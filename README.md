# Kafka-Go
Simple implementation of PubSub using Kafka in Go using IBM/sarama 
(Tutorial by Akhil Sharma)

1 Broker 1 Partiton
```
docker compose up -d
```
```
go run producer/producer.go
go run conmsumer/consumer.go
```

To hit API:
```
curl --location --request POST '0.0.0.0:3000/api/v1/comments' \
--header 'Content-Type: application/json' \
--data-raw '{ "text":"message 1" }'
```

To create topic with options (using docker-compose.multi.yml):
```
docker-compose -f docker-compose.multi.yml up -d
```
```
docker exec -it <kafka1-container-id> kafka-topics --create \
  --bootstrap-server kafka1:9092 \
  --replication-factor 2 \
  --partitions 6 \
  --topic comments
```