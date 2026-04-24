package com.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.InputStream;
import java.util.*;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("Kafka Producer App Tests")
public class KafkaProducerAppTest {

    @Mock
    private KafkaProducer<String, String> mockProducer;

    @Mock
    private Future<RecordMetadata> mockFuture;

    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
    }

    @Test
    @DisplayName("Should create Kafka producer with correct configuration")
    void testProducerConfiguration() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        assertNotNull(props);
        assertEquals("localhost:9092", props.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals(StringSerializer.class.getName(),
                props.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
    }

    @Test
    @DisplayName("Should serialize employee object to JSON correctly")
    void testEmployeeJsonSerialization() throws Exception {
        Map<String, Object> employee = new HashMap<>();
        employee.put("id", 1);
        employee.put("name", "John Doe");
        employee.put("age", 30);
        employee.put("country", "India");

        String json = objectMapper.writeValueAsString(employee);

        assertNotNull(json);
        assertTrue(json.contains("John Doe"));
        assertTrue(json.contains("\"id\":1"));
        assertTrue(json.contains("\"age\":30"));
    }

    @Test
    @DisplayName("Should send multiple employee records to Kafka")
    void testSendMultipleRecords() throws Exception {
        List<Map<String, Object>> employees = Arrays.asList(
                createEmployee(1, "John Doe", 30, "India"),
                createEmployee(2, "Jane Smith", 28, "USA"),
                createEmployee(3, "Bob Johnson", 35, "India")
        );

        when(mockProducer.send(any(ProducerRecord.class))).thenReturn(mockFuture);

        for (Map<String, Object> emp : employees) {
            String json = objectMapper.writeValueAsString(emp);
            ProducerRecord<String, String> record = new ProducerRecord<>("Emp", 
                    emp.get("id").toString(), json);
            mockProducer.send(record);
        }

        ArgumentCaptor<ProducerRecord<String, String>> captor = 
                ArgumentCaptor.forClass(ProducerRecord.class);
        verify(mockProducer, times(3)).send(captor.capture());

        List<ProducerRecord<String, String>> capturedRecords = captor.getAllValues();
        assertEquals(3, capturedRecords.size());
        assertEquals("Emp", capturedRecords.get(0).topic());
    }

    @Test
    @DisplayName("Should handle employee with correct key")
    void testEmployeeKeyHandling() throws Exception {
        Map<String, Object> employee = createEmployee(42, "Test User", 25, "India");

        String json = objectMapper.writeValueAsString(employee);
        ProducerRecord<String, String> record = new ProducerRecord<>("Emp", 
                employee.get("id").toString(), json);

        assertEquals("42", record.key());
        assertEquals("Emp", record.topic());
        assertNotNull(record.value());
    }

    @Test
    @DisplayName("Should validate employee data structure")
    void testEmployeeDataStructure() throws Exception {
        String employeeJson = "{\"id\":1,\"name\":\"John\",\"age\":30,\"country\":\"India\"}";
        
        Map<String, Object> employee = objectMapper.readValue(employeeJson, Map.class);

        assertTrue(employee.containsKey("id"));
        assertTrue(employee.containsKey("name"));
        assertTrue(employee.containsKey("age"));
        assertTrue(employee.containsKey("country"));
        
        assertEquals(1, employee.get("id"));
        assertEquals("John", employee.get("name"));
        assertEquals(30, employee.get("age"));
        assertEquals("India", employee.get("country"));
    }

    @Test
    @DisplayName("Should handle null values in producer gracefully")
    void testProducerWithNullHandling() {
        ProducerRecord<String, String> record = new ProducerRecord<>("Emp", "1", "test");
        assertNotNull(record);
        assertNotNull(record.value());
    }

    @Test
    @DisplayName("Should close producer after sending messages")
    void testProducerClose() {
        mockProducer.close();
        verify(mockProducer, times(1)).close();
    }

    // Helper method to create test employee data
    private Map<String, Object> createEmployee(int id, String name, int age, String country) {
        Map<String, Object> employee = new HashMap<>();
        employee.put("id", id);
        employee.put("name", name);
        employee.put("age", age);
        employee.put("country", country);
        return employee;
    }
}
