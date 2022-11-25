package com.github.antontucek.kinesis.data.streams.kotlin

import java.math.BigDecimal
import java.math.RoundingMode
import java.time.Instant
import java.util.Random

class OrderGenerator {

    private val rg = Random()

    companion object {
        private val SKUS = arrayOf("Boat", "Jet", "Horse", "Truck", "Van")
        private val COUNTRIES = arrayOf("CZ", "DE", "GB", "US")
        private val CUSTOMERS = arrayOf("Brian", "Fred", "James", "Jane", "Katie", "Megan", "Paige", "Richard")
        fun round(f: Float, decimalPlace: Int) = BigDecimal.valueOf(f.toDouble()).setScale(decimalPlace, RoundingMode.HALF_UP).toFloat()
    }

    /**
     * Generates random order with 1-3 lines
     * Probability:
     *    - 1 lines: 70%
     *    - 2 lines: 20%
     *    - 3 lines: 10%
     * @return Random order
     */
    fun randomOrder(): Order {
        val randomNumber = rg.nextInt(10)
        val lines = when {
            (randomNumber < 7) -> 1
            (randomNumber < 9) -> 2
            else -> 3
        }
        val orderItemList = mutableListOf<OrderItem>()
        for (i in 0 until lines) {
            orderItemList.add(OrderItem(SKUS[rg.nextInt(SKUS.size)], round((10.0 + 10.0 * rg.nextFloat()).toFloat(), 2), 1 + rg.nextInt(5)))
        }
        return Order(Instant.now(), COUNTRIES[rg.nextInt(COUNTRIES.size)], CUSTOMERS[rg.nextInt(CUSTOMERS.size)], orderItemList)
    }

}