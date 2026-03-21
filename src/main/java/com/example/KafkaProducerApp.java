package com.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.InputStream;
import java.util.*;

public class KafkaProducerApp {

    public static void main(String[] args) throws Exception {

        // Kafka producer config
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // Read employees.json
        ObjectMapper mapper = new ObjectMapper();
        InputStream is = KafkaProducerApp.class.getClassLoader().getResourceAsStream("employees.json");

        List<Map<String, Object>> employees = mapper.readValue(is, List.class);

        // Send each record to Kafka topic "Emp"
        for (Map<String, Object> emp : employees) {
            String json = mapper.writeValueAsString(emp);
            ProducerRecord<String, String> record =
                    new ProducerRecord<>("Emp", emp.get("id").toString(), json);

            producer.send(record);

            System.out.println("Sent => " + json);
        }

        producer.close();
    }
}
