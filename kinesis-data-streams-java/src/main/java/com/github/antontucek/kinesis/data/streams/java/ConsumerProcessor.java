package com.github.antontucek.kinesis.data.streams.java;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import software.amazon.kinesis.exceptions.InvalidStateException;
import software.amazon.kinesis.exceptions.ShutdownException;
import software.amazon.kinesis.exceptions.ThrottlingException;
import software.amazon.kinesis.lifecycle.events.*;
import software.amazon.kinesis.processor.RecordProcessorCheckpointer;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

import java.util.HashMap;
import java.util.Map;

public class ConsumerProcessor implements ShardRecordProcessor {

    private static final Log LOG = LogFactory.getLog(ConsumerProcessor.class);
    private String kinesisShardId;
    private final Map<String, Double> salesMap = new HashMap<>();

    // Reporting interval
    private static final long REPORTING_INTERVAL_MILLIS = 15000L; // 15 seconds
    private long nextReportingTimeInMillis;

    // Checkpointing interval
    private static final long CHECKPOINT_INTERVAL_MILLIS = 15000L; // 15 seconds
    private long nextCheckpointTimeInMillis;


    @Override
    public void initialize(InitializationInput initializationInput) {
        kinesisShardId = initializationInput.shardId();
        LOG.info("Initializing record processor for shard: " + kinesisShardId);
        LOG.info("Initializing @ Sequence: " + initializationInput.extendedSequenceNumber().toString());

        nextReportingTimeInMillis = System.currentTimeMillis() + REPORTING_INTERVAL_MILLIS;
        nextCheckpointTimeInMillis = System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS;
    }

    @Override
    public void processRecords(ProcessRecordsInput processRecordsInput) {
        try {
            LOG.debug("Processing " + processRecordsInput.records().size() + " record(s)");
            processRecordsInput.records().forEach(this::processRecord);
            // If it is time to report stats as per the reporting interval, report stats
            if (System.currentTimeMillis() > nextReportingTimeInMillis) {
                reportStats();
                resetStats();
                nextReportingTimeInMillis = System.currentTimeMillis() + REPORTING_INTERVAL_MILLIS;
            }

            // Checkpoint once every checkpoint interval
            if (System.currentTimeMillis() > nextCheckpointTimeInMillis) {
                checkpoint(processRecordsInput.checkpointer());
                nextCheckpointTimeInMillis = System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS;
            }
        } catch (Throwable t) {
            LOG.error("Caught throwable while processing records. Aborting.");
            Runtime.getRuntime().halt(1);
        }
    }

    @Override
    public void leaseLost(LeaseLostInput leaseLostInput) {
        LOG.info("Lost lease, so terminating.");
    }

    @Override
    public void shardEnded(ShardEndedInput shardEndedInput) {
        try {
            // Important to checkpoint after reaching end of shard, so we can start processing data from child shards.
            LOG.info("Reached shard end checkpointing.");
            shardEndedInput.checkpointer().checkpoint();
        } catch (ShutdownException | InvalidStateException e) {
            LOG.error("Exception while checkpointing at shard end. Giving up.", e);
        }
    }

    @Override
    public void shutdownRequested(ShutdownRequestedInput shutdownRequestedInput) {
        LOG.info("Scheduler is shutting down, checkpointing.");
        checkpoint(shutdownRequestedInput.checkpointer());
    }

    private void checkpoint(RecordProcessorCheckpointer checkpointer) {
        LOG.info("Checkpointing shard " + kinesisShardId);
        try {
            checkpointer.checkpoint();
        } catch (ShutdownException se) {
            // Ignore checkpoint if the processor instance has been shutdown (fail over).
            LOG.info("Caught shutdown exception, skipping checkpoint.", se);
        } catch (ThrottlingException e) {
            // Skip checkpoint when throttled. In practice, consider a backoff and retry policy.
            LOG.error("Caught throttling exception, skipping checkpoint.", e);
        } catch (InvalidStateException e) {
            // This indicates an issue with the DynamoDB table (check for table, provisioned IOPS).
            LOG.error("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.", e);
        }
    }

    private void processRecord(KinesisClientRecord record) {
        byte[] arr = new byte[record.data().remaining()];
        record.data().get(arr);
        Order order = Order.fromJsonAsBytes(arr);
        if (order == null) {
            LOG.warn("Skipping record. Unable to parse record into Order. Partition Key: " + record.partitionKey());
            return;
        }
        salesMap.put(order.getCountry(), salesMap.getOrDefault(order.getCountry(), 0.00) + order.getSales());
    }

    private void reportStats() {
        for(Map.Entry<String, Double> entry : salesMap.entrySet()) {
            String country = entry.getKey();
            double sales = entry.getValue();
            LOG.info(String.format("%s sales are %.2f", country, sales));
        }
    }

    private void resetStats() {
        salesMap.clear();
    }
}
