package com.practice.kafka.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.practice.kafka.model.OrderModel;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrderSerializer implements Serializer<OrderModel> {

    ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    private static final Logger logger = LoggerFactory.getLogger(OrderSerializer.class);

    @Override
    public byte[] serialize(String topic, OrderModel order) {
        byte[] serializedOrder = null;

        try {
            serializedOrder = objectMapper.writeValueAsBytes(order);
        } catch (JsonProcessingException e) {
            logger.error("Json processing serialization exception:", e.getMessage());
        }
        return serializedOrder;
    }
}
