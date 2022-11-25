package com.github.antontucek.kinesis.data.streams.java;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class OrderGenerator {

    private final Random RANDOM = new Random();
    private final static String[] SKUS = {"Boat", "Jet", "Horse", "Truck", "Van"};
    private final static String[] COUNTRIES = {"CZ", "DE", "GB", "US"};
    private final static  String[] CUSTOMERS = {"Brian", "Fred", "James", "Jane", "Katie", "Megan", "Paige", "Richard"};


    /**
     * Generates random order with 1-3 lines
     * Probability:
     *    - 1 lines: 70%
     *    - 2 lines: 20%
     *    - 3 lines: 10%
     * @return Random order
     */
    public Order getRandomOrder() {
        // Generate OrderItems
        int randomNumber = RANDOM.nextInt(10);
        int lines;
        if (randomNumber < 7) {
            lines = 1;
        } else if (randomNumber < 9) {
            lines = 2;
        } else {
            lines = 3;
        }
        List<OrderItem> orderItemList = new ArrayList<>();
        for (int i = 0; i < lines; i++) {
            orderItemList.add(new OrderItem(SKUS[RANDOM.nextInt(SKUS.length)],
                    round((float) (10.0 + 10.0 * RANDOM.nextFloat()), 2),
                    1 + RANDOM.nextInt(5)));
        }

        // Return
        return new Order(Instant.now(),
                COUNTRIES[RANDOM.nextInt(COUNTRIES.length)],
                CUSTOMERS[RANDOM.nextInt(CUSTOMERS.length)],
                orderItemList);
    }

    /**
     * Rounds float to required decimal place
     * @param f Float value to round
     * @param decimalPlace How many decimal places to round
     * @return Rounded float value
     */
    public static float round(float f, int decimalPlace) {
        return BigDecimal.valueOf(f).setScale(decimalPlace, RoundingMode.HALF_UP).floatValue();
    }
}
