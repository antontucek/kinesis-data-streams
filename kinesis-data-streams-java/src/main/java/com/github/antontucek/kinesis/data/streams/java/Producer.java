package com.github.antontucek.kinesis.data.streams.java;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.kinesis.common.KinesisClientUtil;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Random;
import java.util.concurrent.ExecutionException;


public class Producer {

    private static final Region AWS_REGION = Region.of("eu-central-1");
    private static final String AWS_STREAM_NAME = "OrderStream";
    /** Speed of putting orders to Kinesis stream **/
    private static final int PUT_INTERVAL_MS = 100;
    private static final Log LOG = LogFactory.getLog(Producer.class);
    private static final Random RANDOM = new Random();

    /**
     * Checks if the stream exists and is active
     *
     * @param kinesisClient Amazon Kinesis client instance
     */
    private static void validateStream(KinesisAsyncClient kinesisClient) {
        try {
            DescribeStreamRequest describeStreamRequest =  DescribeStreamRequest.builder().streamName(AWS_STREAM_NAME).build();
            DescribeStreamResponse describeStreamResponse = kinesisClient.describeStream(describeStreamRequest).get();
            if(!describeStreamResponse.streamDescription().streamStatus().toString().equals("ACTIVE")) {
                System.err.println("Stream " + AWS_STREAM_NAME + " is not active. Please wait a few moments and try again.");
                System.exit(1);
            }
        } catch (Exception e) {
            System.err.println("Error found while describing the stream " + AWS_STREAM_NAME);
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }

    /**
     * Sends order to Kinesis stream
     *
     * @param kinesisClient Amazon Kinesis client instance
     * @param order Order instance
     */
    private static void sendOrder(KinesisAsyncClient kinesisClient, Order order) {
        byte[] bytes = order.toJsonAsBytes();
        // The bytes could be null if there is an issue with the JSON serialization by the Jackson JSON library.
        if (bytes == null) {
            LOG.warn("Could not get JSON bytes for stock trade");
            return;
        }
        LOG.info("Putting order: " + order);
        PutRecordRequest request = PutRecordRequest.builder()
                .partitionKey(String.valueOf(RANDOM.nextInt(100))) // We use the ticker symbol as the partition key, explained in the Supplemental Information section below.
                .streamName(AWS_STREAM_NAME)
                .data(SdkBytes.fromByteArray(bytes))
                .build();
        try {
            kinesisClient.putRecord(request).get();
        } catch (InterruptedException e) {
            LOG.info("Interrupted, assuming shutdown.");
        } catch (ExecutionException e) {
            LOG.error("Exception while sending data to Kinesis. Will try again next cycle.", e);
        }
    }

    public static void main(String[] args) throws InterruptedException {
        // Init
        KinesisAsyncClient kinesisClient = KinesisClientUtil.createKinesisAsyncClient(KinesisAsyncClient.builder().region(AWS_REGION));
        validateStream(kinesisClient);

        // Send data to Kinesis
        OrderGenerator orderGenerator = new OrderGenerator();
        while(true) {
            Order order = orderGenerator.getRandomOrder();
            sendOrder(kinesisClient, order);
            Thread.sleep(PUT_INTERVAL_MS);
        }
    }
}
