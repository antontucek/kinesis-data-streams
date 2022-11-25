package com.github.antontucek.kinesis.data.streams.kotlin

import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.kinesis.coordinator.Scheduler
import software.amazon.kinesis.common.ConfigsBuilder
import software.amazon.kinesis.common.KinesisClientUtil
import java.util.UUID
import kotlin.system.exitProcess

fun main() {
    val c = Consumer()
    c.consume()
}

class Consumer {

    private val LOG: Log = LogFactory.getLog(this::class.java.name)
    private val awsRegion = Region.of("eu-central-1")
    private val awsStreamName = "OrderStream"
    private val applicationName = awsStreamName + "KCL"

    fun consume() {
        val kinesisClient = KinesisClientUtil.createKinesisAsyncClient(KinesisAsyncClient.builder().region(awsRegion))
        val dynamoClient = DynamoDbAsyncClient.builder().region(awsRegion).build()
        val cloudWatchClient = CloudWatchAsyncClient.builder().region(awsRegion).build()
        val shardRecordProcessor = ConsumerFactory()
        val configsBuilder = ConfigsBuilder(awsStreamName, applicationName, kinesisClient, dynamoClient, cloudWatchClient, UUID.randomUUID().toString(), shardRecordProcessor)

        val scheduler = Scheduler(
            configsBuilder.checkpointConfig(),
            configsBuilder.coordinatorConfig(),
            configsBuilder.leaseManagementConfig(),
            configsBuilder.lifecycleConfig(),
            configsBuilder.metricsConfig(),
            configsBuilder.processorConfig(),
            configsBuilder.retrievalConfig()
        )
        var exitCode = 0

        try {
            scheduler.run()
        } catch (t: Throwable) {
            LOG.error("Caught throwable while processing data.", t)
            exitCode = 1
        }
        exitProcess(exitCode)
    }

}

