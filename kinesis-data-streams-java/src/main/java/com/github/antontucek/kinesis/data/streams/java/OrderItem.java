package com.github.antontucek.kinesis.data.streams.java;

import java.util.Objects;

public class OrderItem {

    private String sku;
    private float price;
    private int quantity;

    /**
     * Empty constructor is required for JSON.readValue
     */
    private OrderItem() {

    }

    public OrderItem(String sku, float price, int quantity) {
        this.sku = sku;
        this.price = price;
        this.quantity = quantity;
    }

    public String getSku() {
        return sku;
    }

    public void setSku(String sku) {
        this.sku = sku;
    }

    public float getPrice() {
        return price;
    }

    public void setPrice(float price) {
        this.price = price;
    }

    public int getQuantity() {
        return quantity;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }

    @Override
    public String toString() {
        return "OrderItem{" +
                "sku='" + sku + '\'' +
                ", price=" + price +
                ", quantity=" + quantity +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof OrderItem orderItem)) return false;
        return Float.compare(orderItem.price, price) == 0 && quantity == orderItem.quantity && Objects.equals(sku, orderItem.sku);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sku, price, quantity);
    }
}
