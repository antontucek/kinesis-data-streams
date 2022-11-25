# Summary
The solution contains:
- [kinesis+iam.yaml](kinesis+iam.yaml) - infrastructure of AWS Kinesis Data Streams + AWS IAM minimum privileges.
- [kinesis.data.streams.java](kinesis-data-streams-java/src/main/java/com/github/antontucek/kinesis/data/streams/java/) version, executable files:
  - `Producer.java` – application putting data into the stream.
  - `Consumer.java` – collector of stream data with 15-seconds aggregation.
- [kinesis.data.streams.kotlin](kinesis-data-streams-kotlin/src/main/kotlin/com/github/antontucek/kinesis/data/streams/kotlin/) version, executable files:
  - `Producer.kt` – application putting data into the stream.
  - `Consumer.kt` – collector of stream data with 15-seconds aggregation.
The solution is based on [AWS Tutorial](https://docs.aws.amazon.com/streams/latest/dev/tutorial-stock-data-kplkcl2.html).

# Prerequisites
- AWS Account.
- Java SDK.
- Java IDE – JetBrains IntelliJ preferred.

# Installation
- Login to your AWS account.
- Add new Cloudformation stack, use [kinesis+iam.yaml](kinesis+iam.yaml) template.
- Add `KinesisConsumerRole` role to consumer instance.
- Add `KinesisProducerRole` role to producer instance.

Usage Java
- Run `kinesis.data.streams.java.Producer` to produce messages.
- Run `kinesis.data.streams.java.Consumer` to process messages.

Usage Kotlin
- Run `kinesis.data.streams.kotlin.Producer` to produce messages.
- Run `kinesis.data.streams.kotlin.Consumer` to process messages.