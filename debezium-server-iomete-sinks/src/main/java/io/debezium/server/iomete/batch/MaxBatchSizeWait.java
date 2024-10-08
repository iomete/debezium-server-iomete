package io.debezium.server.iomete.batch;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.server.iomete.DebeziumMetrics;

import javax.enterprise.context.Dependent;
import javax.inject.Inject;
import javax.inject.Named;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Dependent
@Named("MaxBatchSizeWait")
public class MaxBatchSizeWait implements InterfaceBatchSizeWait {
    protected static final Logger LOGGER = LoggerFactory.getLogger(MaxBatchSizeWait.class);
    @ConfigProperty(name = "debezium.source.max.batch.size", defaultValue = CommonConnectorConfig.DEFAULT_MAX_BATCH_SIZE + "")
    int maxBatchSize;
    @ConfigProperty(name = "debezium.sink.batch.batch-size-wait.max-wait-ms", defaultValue = "300000")
    int maxWaitMs;
    @ConfigProperty(name = "debezium.sink.batch.batch-size-wait.wait-interval-ms", defaultValue = "10000")
    int waitIntervalMs;
    @Inject
    DebeziumMetrics debeziumMetrics;

    @Override
    public void init() throws DebeziumException {
        assert waitIntervalMs < maxWaitMs : "`wait-interval-ms` cannot be bigger than `max-wait-ms`";
        debeziumMetrics.initizalize();
    }

    @Override
    public void waitMs(long numRecordsProcessed, Integer processingTimeMs) throws InterruptedException {

        if (debeziumMetrics.snapshotRunning()) {
            return;
        }

        LOGGER.info("Processed {}, " +
                        "QueueCurrentSize:{}, QueueTotalCapacity:{}, " +
                        "QueueCurrentUtilization:{}%" +
                        "SecondsBehindSource:{}, " +
                        "SnapshotCompleted:{}, snapshotRunning:{}",
                numRecordsProcessed,
                debeziumMetrics.streamingQueueCurrentSize(), debeziumMetrics.maxQueueSize(),
                (debeziumMetrics.streamingQueueCurrentSize() / debeziumMetrics.maxQueueSize()) * 100,
                (int) (debeziumMetrics.streamingMilliSecondsBehindSource() / 1000),
                debeziumMetrics.snapshotCompleted(), debeziumMetrics.snapshotRunning()
        );

        int totalWaitMs = 0;
        while (totalWaitMs < maxWaitMs && debeziumMetrics.streamingQueueCurrentSize() < maxBatchSize) {
            totalWaitMs += waitIntervalMs;
            LOGGER.trace("Sleeping {} Milliseconds, QueueCurrentSize:{} < maxBatchSize:{}, Total wait {}",
                    waitIntervalMs, debeziumMetrics.streamingQueueCurrentSize(), maxBatchSize, totalWaitMs);

            Thread.sleep(waitIntervalMs);
        }

        LOGGER.debug("Total wait {} Milliseconds, QueueCurrentSize:{}, maxBatchSize:{}",
                totalWaitMs, debeziumMetrics.streamingQueueCurrentSize(), maxBatchSize);

    }

}
