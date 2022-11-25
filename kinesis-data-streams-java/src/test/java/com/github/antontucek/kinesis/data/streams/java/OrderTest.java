package com.github.antontucek.kinesis.data.streams.java;

import org.junit.jupiter.api.Assertions;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;


class OrderTest {

    @org.junit.jupiter.api.Test
    void fromToJsonAsBytes() {
        Instant instant = LocalDateTime.of(2001, 1, 1, 0, 0).atZone(ZoneId.of("UTC")).toInstant();
        OrderItem orderItem = new OrderItem("Jet", 1000.0F, 1);
        Order order1 = new Order(instant, "CZ", "Jan", List.of(orderItem));
        Order order2 = Order.fromJsonAsBytes(order1.toJsonAsBytes());
        Assertions.assertEquals(order1, order2);
    }
}