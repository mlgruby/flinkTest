# FlinkKafka Job Assignment

## Requirement
- Java 11
- SBT 1.10.2
- Python >= 3.8
- Docker
- Docker Compose

## How to run
1. Run `docker-compose up` to start Kafka and Zookeeper services
2. The above command also starts the kafka producer which produces messages to the topic `input-topic`
3. This also starts a dummy enricher service which returns a random text for the given message ID
4. Run the following command to start the Flink job
```shell
 sbt "runMain org.flink.assignment.FlinkKafkaAssignmentJob"
```
5. The Flink job reads messages from the topic `input-topic` and writes the messages to the topic `output-topic`
    - In between, the job perform the following operations
        - Debounce the messages by 2 seconds window (This is configurable)
        - Append the message with the current timestamp
        - Enrich the message with a random text
6. The enriched messages are written to the topic `output-topic`

## How to run test
1. Run the following command to start the test
```shell
 sbt clean test
```
Note: The test cases are written for the `MessageTransformer` class

## Code Structure
Following is the code structure of the project
```
src
├── main
│   ├── resources
│   │   └── flink-kafka-job.properties - (Contains the configuration for the Flink job)
│   └── scala
│       └── org
│           └── flink
│               └── assignment
│                   ├── config
│                   │   └── ConfigManager.scala - (Reads the configuration from the properties file)
│                   └── jobs
│                       └── flinkKafka
│                           ├── DedupWindowFunction.scala - (Debounce the messages by 2 seconds window and also performs the dedup operation)
│                           ├── FlinkKafkaJob.scala - (Contains the main Flink Kafka job)
│                           ├── FlinkKafkaJobConfig.scala - (Contains the configuration for the Flink job)
│                           ├── HttpEnrichmentFunction.scala - (Enrich the message with a random text)
│                           ├── JSONSerde.scala - (Contains the JSON serde for the Kafka messages)
│                           ├── MessageTransformer.scala - (Contains the transformation logic for the Flink job)
│                           └── FlinkJob.scala - (Trait for the flink job creation)
│                   └── FlinkKafkaAssignmentJob.scala - (Main class to start the Flink job)
└── test
    └── scala
        └── org
            └── flink
                └── assignment
                    └── jobs
                        └── flinkKafka
                            └── MessageTransformerSuite.scala - (Contains the test cases for the MessageTransformer)
```

## Configuration
Adjust settings in `src/main/resources/flink-kafka-job.properties` as needed.

## Key Components
- `ConfigManager`: Manages job configuration
- `DedupWindowFunction`: Handles message debouncing and deduplication
- `FlinkKafkaJob`: Core job logic
- `HttpEnrichmentFunction`: Enriches messages with external data
- `MessageTransformer`: Applies transformations to messages
- `FlinkKafkaAssignmentJob`: Entry point for job execution