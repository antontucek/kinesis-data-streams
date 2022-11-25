package com.github.antontucek.kinesis.data.streams.kotlin

import com.fasterxml.jackson.module.kotlin.readValue
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.LocalDateTime
import java.time.ZoneId

class OrderTest {

    @Test
    fun fromToJsonAsBytes() {
        val instant = LocalDateTime.of(2001, 1, 1, 0, 0).atZone(ZoneId.of("UTC")).toInstant()
        val orderItem = OrderItem("Jet", 1000.0F, 1)
        val order1 = Order(instant, "CZ", "Jan", listOf(orderItem))
        val order2: Order = Order.mapper.readValue(Order.mapper.writeValueAsBytes(order1))
        assertThat(order1).isEqualTo(order2)
    }
}