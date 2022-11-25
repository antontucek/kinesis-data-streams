package com.github.antontucek.kinesis.data.streams.java;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

public class Order {

    private final static ObjectMapper JSON = new ObjectMapper();
    static {
        JSON.registerModule(new JavaTimeModule());
    }

    @JsonFormat(shape=JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss", timezone = "UTC")
    private Instant date;
    private String country;
    private String customer;
    private List<OrderItem> orderItemList;

    /**
     * Empty constructor is required for JSON.readValue
     */
    private Order() {

    }

    public Order(Instant date, String country, String customer, List<OrderItem> orderItemList) {
        this.date = date;
        this.country = country;
        this.customer = customer;
        this.orderItemList = orderItemList;
    }

    public Instant getDate() {
        return date;
    }

    public void setDate(Instant date) {
        this.date = date;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getCustomer() {
        return customer;
    }

    public void setCustomer(String customer) {
        this.customer = customer;
    }

    public List<OrderItem> getOrderItemList() {
        return orderItemList;
    }

    public void setOrderItemList(List<OrderItem> orderItemList) {
        this.orderItemList = orderItemList;
    }

    @JsonIgnore
    public double getSales() {
        double result = 0.00;
        for (OrderItem oi: orderItemList) {
            result += oi.getPrice() * oi.getQuantity();
        }
        return result;
    }

    @Override
    public String toString() {
        return "Order{" +
                "date=" + date +
                ", country='" + country + '\'' +
                ", customer='" + customer + '\'' +
                ", lines=" + (orderItemList != null ? orderItemList.size() : 0) +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Order order)) return false;
        return Objects.equals(date, order.date) && Objects.equals(country, order.country) && Objects.equals(customer, order.customer) && Objects.equals(orderItemList, order.orderItemList);
    }

    @Override
    public int hashCode() {
        return Objects.hash(date, country, customer, orderItemList);
    }

    /**
     * @return Order converted to JSON bytes
     */
    public byte[] toJsonAsBytes() {
        try {
            return JSON.writeValueAsBytes(this);
        } catch (IOException e) {
            return null;
        }
    }

    /**
     * @param bytes Order converted to JSON bytes
     * @return Order instance
     */
    public static Order fromJsonAsBytes(byte[] bytes) {
        try {
            return JSON.readValue(bytes, Order.class);
        } catch (IOException e) {
            return null;
        }
    }

}
