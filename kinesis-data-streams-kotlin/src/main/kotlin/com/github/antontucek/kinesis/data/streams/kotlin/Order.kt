package com.github.antontucek.kinesis.data.streams.kotlin

import com.fasterxml.jackson.annotation.JsonFormat
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import java.time.Instant
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule


data class Order(@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss", timezone = "UTC") var date: Instant,
                 var country: String,
                 var customer: String,
                 var orderItemList: List<OrderItem>) {

    fun sales(): Double {
        var result = 0.00
        for (oi in orderItemList) {
            result += oi.price * oi.quantity
        }
        return result
    }

    companion object {
        val mapper = jacksonObjectMapper()
        init {
            mapper.registerModule(JavaTimeModule())
        }
    }

}