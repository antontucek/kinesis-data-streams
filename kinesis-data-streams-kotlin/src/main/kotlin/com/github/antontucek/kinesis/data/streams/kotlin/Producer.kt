package com.github.antontucek.kinesis.data.streams.kotlin

import org.apache.commons.logging.LogFactory
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest
import software.amazon.kinesis.common.KinesisClientUtil
import java.util.*
import java.util.concurrent.ExecutionException
import kotlin.system.exitProcess



fun main() {
    // Init
    val putIntervalMs = 100L // Speed of putting orders to Kinesis stream
    val awsRegion = Region.of("eu-central-1")
    val p = Producer()
    val kinesisClient = KinesisClientUtil.createKinesisAsyncClient(KinesisAsyncClient.builder().region(awsRegion))
    p.validateStream(kinesisClient)

    // Send data to Kinesis
    val orderGenerator = OrderGenerator()
    while(true) {
        val order = orderGenerator.randomOrder()
        p.sendOrder(kinesisClient, order)
        Thread.sleep(putIntervalMs)
    }
}

class Producer {

    private val awsStreamName = "OrderStream"
    private val LOG = LogFactory.getLog(this::class.java.name)
    private val RANDOM = Random()

    /**
     * Checks if the stream exists and is active
     *
     * @param kinesisClient Amazon Kinesis client instance
     */
    fun validateStream(kinesisClient: KinesisAsyncClient) {
        try {
            val describeStreamRequest =  DescribeStreamRequest.builder().streamName(awsStreamName).build()
            val describeStreamResponse = kinesisClient.describeStream(describeStreamRequest).get()
            if(describeStreamResponse.streamDescription().streamStatus().toString() != "ACTIVE") {
                System.err.println("Stream $awsStreamName is not active. Please wait a few moments and try again.")
                exitProcess(1)
            }
        } catch (e: Exception) {
            System.err.println("Error found while describing the stream $awsStreamName")
            System.err.println(e.toString())
            exitProcess(1)
        }
    }

    /**
     * Sends order to Kinesis stream
     *
     * @param kinesisClient Amazon Kinesis client instance
     * @param order Order instance
     */
    fun sendOrder(kinesisClient: KinesisAsyncClient, order: Order) {
        val bytes = Order.mapper.writeValueAsBytes(order)
        // The bytes could be null if there is an issue with the JSON serialization by the Jackson JSON library.
        if (bytes == null) {
            LOG.warn("Could not get JSON bytes for stock trade")
            return
        }
        LOG.info("Putting order: $order")
        val request = PutRecordRequest.builder()
            .partitionKey(RANDOM.nextInt(100).toString()) // We use the ticker symbol as the partition key, explained in the Supplemental Information section below.
            .streamName(awsStreamName)
            .data(SdkBytes.fromByteArray(bytes))
            .build()
        try {
            kinesisClient.putRecord(request).get()
        } catch (e: InterruptedException) {
            LOG.info("Interrupted, assuming shutdown.")
        } catch (e: ExecutionException) {
            LOG.error("Exception while sending data to Kinesis. Will try again next cycle.", e)
        }
    }
}