package com.example;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Map;

public class TestDataBuilder {

    private static final ObjectMapper mapper = new ObjectMapper();

    public static Map<String, Object> createEmployee(int id, String name, int age, String country) {
        Map<String, Object> employee = new HashMap<>();
        employee.put("id", id);
        employee.put("name", name);
        employee.put("age", age);
        employee.put("country", country);
        return employee;
    }

    public static String employeeToJson(Map<String, Object> employee) throws Exception {
        return mapper.writeValueAsString(employee);
    }

    public static Map<String, Object> jsonToEmployee(String json) throws Exception {
        return mapper.readValue(json, Map.class);
    }

    public static String createEmployeeJson(int id, String name, int age, String country) throws Exception {
        return employeeToJson(createEmployee(id, name, age, country));
    }

    public static Map<String, Object>[] createEmployees(int count) {
        @SuppressWarnings("unchecked")
        Map<String, Object>[] employees = new Map[count];
        String[] countries = {"India", "USA", "UK", "Canada", "Australia"};
        String[] names = {"John", "Jane", "Bob", "Alice", "Charlie"};

        for (int i = 0; i < count; i++) {
            employees[i] = createEmployee(
                    i + 1,
                    names[i % names.length] + (i / names.length),
                    20 + (i % 40),
                    countries[i % countries.length]
            );
        }
        return employees;
    }
}
