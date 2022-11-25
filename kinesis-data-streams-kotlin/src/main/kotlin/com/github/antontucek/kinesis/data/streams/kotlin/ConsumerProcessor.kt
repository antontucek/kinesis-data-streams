package com.github.antontucek.kinesis.data.streams.kotlin

import com.fasterxml.jackson.module.kotlin.readValue
import org.apache.commons.logging.LogFactory
import software.amazon.kinesis.exceptions.InvalidStateException
import software.amazon.kinesis.exceptions.ShutdownException
import software.amazon.kinesis.exceptions.ThrottlingException
import software.amazon.kinesis.lifecycle.events.*
import software.amazon.kinesis.processor.RecordProcessorCheckpointer
import software.amazon.kinesis.processor.ShardRecordProcessor
import software.amazon.kinesis.retrieval.KinesisClientRecord


private const val REPORTING_INTERVAL_MILLIS = 15000L // Reporting interval
private const val CHECKPOINT_INTERVAL_MILLIS = 15000L // Checkpointing interval

class ConsumerProcessor: ShardRecordProcessor {

    private val LOG = LogFactory.getLog(this::class.java.name)
    private var kinesisShardId: String = ""
    private var salesMap = HashMap<String, Double>()
    private var nextReportingTimeInMillis = 0L
    private var nextCheckpointTimeInMillis = 0L

    override fun initialize(initializationInput: InitializationInput) {
        kinesisShardId = initializationInput.shardId()
        LOG.info("Initializing record processor for shard: $kinesisShardId")
        LOG.info("Initializing @ Sequence: ${initializationInput.extendedSequenceNumber()}")

        nextReportingTimeInMillis = System.currentTimeMillis() + REPORTING_INTERVAL_MILLIS
        nextCheckpointTimeInMillis = System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS
    }

    override fun processRecords(processRecordsInput: ProcessRecordsInput) {
        try {
            LOG.debug("Processing ${processRecordsInput.records().size} record(s)")
            processRecordsInput.records().forEach(this::processRecord)
            // If it is time to report stats as per the reporting interval, report stats
            if (System.currentTimeMillis() > nextReportingTimeInMillis) {
                reportStats()
                resetStats()
                nextReportingTimeInMillis = System.currentTimeMillis() + REPORTING_INTERVAL_MILLIS
            }

            // Checkpoint once every checkpoint interval
            if (System.currentTimeMillis() > nextCheckpointTimeInMillis) {
                checkpoint(processRecordsInput.checkpointer())
                nextCheckpointTimeInMillis = System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS
            }
        } catch (t: Throwable) {
            LOG.error("Caught throwable while processing records. Aborting.")
            Runtime.getRuntime().halt(1)
        }
    }

    override fun leaseLost(leaseLostInput: LeaseLostInput?) {
        LOG.info("Lost lease, so terminating.")
    }

    override fun shardEnded(shardEndedInput: ShardEndedInput) {
        try {
            // Important to checkpoint after reaching end of shard, so we can start processing data from child shards.
            LOG.info("Reached shard end checkpointing.")
            shardEndedInput.checkpointer().checkpoint()
        } catch (e: ShutdownException) {
            LOG.error("Exception while checkpointing at shard end. Giving up.", e)
        } catch (e: InvalidStateException) {
            LOG.error("Exception while checkpointing at shard end. Giving up.", e)
        }
    }

    override fun shutdownRequested(shutdownRequestedInput: ShutdownRequestedInput) {
        LOG.info("Scheduler is shutting down, checkpointing.")
        checkpoint(shutdownRequestedInput.checkpointer())
    }

    private fun checkpoint(checkpointer: RecordProcessorCheckpointer) {
        LOG.info("Checkpointing shard $kinesisShardId")
        try {
            checkpointer.checkpoint()
        } catch (se: ShutdownException) {
            // Ignore checkpoint if the processor instance has been shutdown (fail over).
            LOG.info("Caught shutdown exception, skipping checkpoint.", se)
        } catch (e: ThrottlingException) {
            // Skip checkpoint when throttled. In practice, consider a backoff and retry policy.
            LOG.error("Caught throttling exception, skipping checkpoint.", e)
        } catch (e: InvalidStateException) {
            // This indicates an issue with the DynamoDB table (check for table, provisioned IOPS).
            LOG.error("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.", e)
        }
    }

    private fun processRecord(record: KinesisClientRecord) {
        val arr = ByteArray(record.data().remaining())
        record.data().get(arr)
        val order: Order? = Order.mapper.readValue(arr)
        if (order == null) {
            LOG.warn("Skipping record. Unable to parse record into Order. Partition Key: ${record.partitionKey()}")
            return
        }
        salesMap[order.country] = salesMap.getOrDefault(order.country, 0.00) + order.sales()
    }

    private fun reportStats() {
        for(entry in salesMap.entries) {
            val country = entry.key
            val sales = entry.value
            LOG.info(String.format("%s sales are %.2f", country, sales))
        }
    }

    private fun resetStats() {
        salesMap.clear()
    }
}