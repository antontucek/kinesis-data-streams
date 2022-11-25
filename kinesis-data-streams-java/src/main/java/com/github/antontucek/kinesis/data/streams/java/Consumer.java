package com.github.antontucek.kinesis.data.streams.java;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.common.KinesisClientUtil;
import software.amazon.kinesis.coordinator.Scheduler;

import java.util.UUID;

public class Consumer {

    private static final Region AWS_REGION = Region.of("eu-central-1");
    private static final String AWS_STREAM_NAME = "OrderStream";
    private static final String APPLICATION_NAME = AWS_STREAM_NAME + "KCL";
    private static final Log LOG = LogFactory.getLog(Producer.class);

    public static void main(String[] args) {

        KinesisAsyncClient kinesisClient = KinesisClientUtil.createKinesisAsyncClient(KinesisAsyncClient.builder().region(AWS_REGION));
        DynamoDbAsyncClient dynamoClient = DynamoDbAsyncClient.builder().region(AWS_REGION).build();
        CloudWatchAsyncClient cloudWatchClient = CloudWatchAsyncClient.builder().region(AWS_REGION).build();
        ConsumerFactory shardRecordProcessor = new ConsumerFactory();
        ConfigsBuilder configsBuilder = new ConfigsBuilder(AWS_STREAM_NAME, APPLICATION_NAME, kinesisClient, dynamoClient, cloudWatchClient, UUID.randomUUID().toString(), shardRecordProcessor);

        Scheduler scheduler = new Scheduler(
                configsBuilder.checkpointConfig(),
                configsBuilder.coordinatorConfig(),
                configsBuilder.leaseManagementConfig(),
                configsBuilder.lifecycleConfig(),
                configsBuilder.metricsConfig(),
                configsBuilder.processorConfig(),
                configsBuilder.retrievalConfig()
        );
        int exitCode = 0;

        try {
            scheduler.run();
        } catch (Throwable t) {
            LOG.error("Caught throwable while processing data.", t);
            exitCode = 1;
        }
        System.exit(exitCode);

    }

}
