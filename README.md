[![License](http://img.shields.io/:license-apache%202.0-brightgreen.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)
![contributions welcome](https://img.shields.io/badge/contributions-welcome-brightgreen.svg?style=flat)

# Debezium iomete consumers

This Spark App stream CDC events from MySQL, PostgreSQL, and etc. to iomete lakehouse running [Debezium](https://debezium.io/documentation/reference/operations/debezium-server.html) under the hood. 
This spark application/consumer can consume CDC events as mini batches

![Debezium iomete spark application](./docs/images/iomete-debezium.svg)


## `iomete` Consumer

Consumes debezium events using spark

| Config                                              | Default              | Description                                                                       |
|-----------------------------------------------------|----------------------|-----------------------------------------------------------------------------------|
| `debezium.sink.iomete.upsert`                       | `false`              | Save mode is upsert or append                                                     |
| `debezium.sink.iomete.upsert-keep-deletes`          | `true`               | On upsert mode, deleted events/rows should be kept or deleted                     |
| `debezium.sink.iomete.table-namespace`              | `default`            | Database name (namespace) name where the destination tables are (will be created) |
| `debezium.sink.iomete.destination-regexp`           | ``                   | Regexp to modify destination                                                      |
| `debezium.sink.iomete.destination-regexp-replace`   | ``                   | Regexp Replace part to modify destination                                         |
| `debezium.sink.iomete.table-prefix`                 | ``                   | Table prefix                                                                      |
| `debezium.sink.iomete.table.partition.configs`      | ``                   | List of table names that will be partitioned by day                               |
| `debezium.sink.batch.batch-size-wait`               | `NoBatchSizeWait`    | Batch size wait strategy to optimize data files and upload interval. explained below. |

### Mandatory config

#### Debezium Event format and schema

```properties
debezium.format.value=json
debezium.format.key=json
debezium.format.schemas.enable=true
```

#### Flattening Event Data

Batch consumer requires event flattening, please
see [debezium feature](https://debezium.io/documentation/reference/configuration/event-flattening.html#_configuration)

```properties
debezium.transforms=unwrap
debezium.transforms.unwrap.type=io.debezium.transforms.ExtractNewRecordState
debezium.transforms.unwrap.add.fields=op,table,lsn,source.ts_ms
debezium.transforms.unwrap.add.headers=db
debezium.transforms.unwrap.delete.handling.mode=rewrite
```

### Optimizing batch size (or commit interval)

Debezium extracts database events in real time and this could cause too frequent commits or too many small files
which is not optimal for batch processing especially when near realtime data feed is sufficient.
To avoid this problem following batch-size-wait classes are used.

Batch size wait adds delay between consumer calls to increase total number of events received per call and meanwhile
events are collected in memory.
This setting should be configured together with `debezium.source.max.queue.size` and `debezium.source.max.batch.size`
debezium properties

#### NoBatchSizeWait

This is default configuration, by default consumer will not use any wait. All the events are consumed immediately.

#### MaxBatchSizeWait

MaxBatchSizeWait uses debezium metrics to optimize batch size.
MaxBatchSizeWait periodically reads streaming queue current size and waits until it reaches to `max.batch.size`.
Maximum wait and check intervals are controlled by `debezium.sink.batch.batch-size-wait.max-wait-ms`
, `debezium.sink.batch.batch-size-wait.wait-interval-ms` properties.

example setup to receive ~2048 events per commit. maximum wait is set to 30 seconds, streaming queue current size
checked every 5 seconds

```properties
debezium.sink.batch.batch-size-wait=MaxBatchSizeWait
debezium.sink.batch.metrics.snapshot-mbean=debezium.postgres:type=connector-metrics,context=snapshot,server=testc
debezium.sink.batch.metrics.streaming-mbean=debezium.postgres:type=connector-metrics,context=streaming,server=testc
debezium.source.connector.class=io.debezium.connector.postgresql.PostgresConnector
debezium.source.max.batch.size=2048;
debezium.source.max.queue.size=16000";
debezium.sink.batch.batch-size-wait.max-wait-ms=30000
debezium.sink.batch.batch-size-wait.wait-interval-ms=5000
```

## Configuring log levels

```properties
quarkus.log.level=INFO
# Change this to set Spark log level
quarkus.log.category."org.apache.spark".level=WARN
# hadoop, parquet
quarkus.log.category."org.apache.hadoop".level=WARN
quarkus.log.category."org.apache.parquet".level=WARN
# Ignore messages below warning level from Jetty, because it's a bit verbose
quarkus.log.category."org.eclipse.jetty".level=WARN
#
```