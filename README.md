# Kafka → Spark → MongoDB (Streaming Pipeline)

Simple real-time pipeline using Kafka, Spark Structured Streaming, and MongoDB.

## Flow

JSON → Kafka (Emp) → Spark → Filter (India) → MongoDB

<img width="750" height="340" alt="image" src="https://github.com/user-attachments/assets/fb390b7e-e598-4fd3-aeb3-57242f7d50e9" />



## Tech

Java, Kafka, Spark, MongoDB

## How it works

* Producer reads `employees.json` and sends to Kafka
* Spark consumes messages from topic `Emp`
* Parses JSON and filters `country = India`
* Writes filtered data to MongoDB (`adarsh.india`)

## Run

1. Start Kafka (`localhost:9092`)
2. Start MongoDB (`localhost:27017`)
3. Run `KafkaProducerApp`
4. Run `KafkaMessagesConsume`

## Sample Data

```
{
  "id": 1,
  "name": "Adarsh",
  "age": 24,
  "country": "India"
}
```

## Output

MongoDB → `adarsh.india`

---
