package com.github.antontucek.kinesis.data.streams.kotlin

import software.amazon.kinesis.processor.ShardRecordProcessorFactory

class ConsumerFactory: ShardRecordProcessorFactory {

    override fun shardRecordProcessor() = ConsumerProcessor()
}