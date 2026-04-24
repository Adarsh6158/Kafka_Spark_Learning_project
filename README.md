<h1 align="center">Kafka → Spark → MongoDB (Streaming Pipeline)</h1>
<p align="center">
  <img src="https://img.shields.io/badge/Architecture-Streaming%20Pipeline-blue" />
  <img src="https://img.shields.io/badge/Tech-Apache%20Kafka-black" />
  <img src="https://img.shields.io/badge/Processing-Spark%20Structured%20Streaming-orange" />
  <img src="https://img.shields.io/badge/Database-MongoDB-green" />
  <img src="https://img.shields.io/badge/Language-Java-red" />
</p>

Simple real-time pipeline using Kafka, Spark Structured Streaming, and MongoDB.

## Flow

JSON → Kafka (Emp) → Spark → Filter (India) → MongoDB

<img width="650" height="310" alt="image" src="https://github.com/user-attachments/assets/9ca80721-bffe-45ed-9270-3a6e9dd043d6" />

## Sequence Flow

<img width="900" height="780" alt="image" src="https://github.com/user-attachments/assets/304f431f-43c5-4144-8db1-d7c2ecedba48" />


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
