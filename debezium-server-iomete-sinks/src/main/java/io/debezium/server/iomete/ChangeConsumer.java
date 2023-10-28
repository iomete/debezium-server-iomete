package io.debezium.server.iomete;

import com.esotericsoftware.kryo.util.ObjectMap;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.RecordChangeEvent;
import io.debezium.engine.format.Json;
import io.debezium.serde.DebeziumSerdes;
import io.debezium.server.BaseChangeConsumer;
import io.debezium.server.iomete.batch.BatchSizeWaitUtil;
import io.debezium.server.iomete.batch.InterfaceBatchSizeWait;
import io.debezium.util.Clock;
import io.debezium.util.Strings;
import io.debezium.util.Threads;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.Dependent;
import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import javax.inject.Named;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Named("iomete")
@Dependent
public class ChangeConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {
    protected final Logger logger = LoggerFactory.getLogger(getClass());

    protected static final Duration LOG_INTERVAL = Duration.ofMinutes(15);

    protected static final Object uploadLock = new Object();



    protected static final Serde<String> valSerde = DebeziumSerdes.payloadJson(String.class);
    protected static final Serde<String> keySerde = DebeziumSerdes.payloadJson(String.class);
    static Deserializer<String> keyDeserializer;
    protected Deserializer<String> valDeserializer;

    protected final Clock clock = Clock.system();

    protected long consumerStart = clock.currentTimeInMillis();
    protected long numConsumedEvents = 0;
    protected Threads.Timer logTimer = Threads.timer(clock, LOG_INTERVAL);
    @ConfigProperty(name = "debezium.format.value", defaultValue = "json")
    String valueFormat;
    @ConfigProperty(name = "debezium.format.key", defaultValue = "json")
    String keyFormat;
    @ConfigProperty(name = "debezium.sink.batch.batch-size-wait", defaultValue = "NoBatchSizeWait")
    String batchSizeWaitName;



    @Inject
    @Any
    Instance<InterfaceBatchSizeWait> batchSizeWaitInstances;
    InterfaceBatchSizeWait batchSizeWait;

    @Inject
    protected SparkSession spark;
    @Inject
    TableHandler tableHandler;

    public void init() throws InterruptedException {
        logger.info("init");
        if (!valueFormat.equalsIgnoreCase(Json.class.getSimpleName().toLowerCase())) {
            throw new InterruptedException("debezium.format.value={" + valueFormat + "} not supported! " +
                    "Supported (debezium.format.value=*) formats are {json,}!");
        }

        if (!keyFormat.equalsIgnoreCase(Json.class.getSimpleName().toLowerCase())) {
            throw new InterruptedException("debezium.format.key={" + valueFormat + "} not supported! " +
                    "Supported (debezium.format.key=*) formats are {json,}!");
        }

        // configure and set
        valSerde.configure(Collections.emptyMap(), false);
        valDeserializer = valSerde.deserializer();
        // configure and set
        keySerde.configure(Collections.emptyMap(), true);
        keyDeserializer = keySerde.deserializer();

        tableHandler.createTableIfNotExists();

        batchSizeWait = BatchSizeWaitUtil.selectInstance(batchSizeWaitInstances, batchSizeWaitName);
        logger.info("Using {} to optimize batch size", batchSizeWait.getClass().getSimpleName());
        batchSizeWait.init();
    }

    @PostConstruct
    void connect() throws java.lang.InterruptedException {
        this.init();
    }

    @PreDestroy
    void close() {
        this.stopSparkSession();
    }

    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records, DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {
        logger.trace("Received {} events", records.size());

        Instant start = Instant.now();

        List<CdcEvent> events = records.stream().map(CdcEvent::new).collect(Collectors.toList());
        long numUploadedEvents = this.uploadDestination(events);
        for (ChangeEvent<Object, Object> record : records) {
            logger.trace("Processed event '{}'", record);
            committer.markProcessed(record);
        }
        committer.markBatchFinished();

        this.logConsumerProgress(numUploadedEvents);

        logger.debug("Received:{} Processed:{} events", records.size(), numUploadedEvents);
        batchSizeWait.waitMs(numUploadedEvents, (int) Duration.between(start, Instant.now()).toMillis());
    }

    private long uploadDestination(List<CdcEvent> events) {
        try {
            Instant start = Instant.now();

            List<Row> rows = new ArrayList<>();
            for (CdcEvent event : events) {
                rows.add(RowFactory.create(
                        event.getSourceServer(),
                        event.getSourceTopic(),
                        event.getSourceOffsetTsSec(),
                        event.getSourceOffsetFile(),
                        event.getSourceOffsetPos(),
                        event.getSourceOffsetSnapshot(),
                        Instant.ofEpochSecond(event.getSourceOffsetTsSec()),
                        event.getKey(),
                        event.getValue(),
                        event.getProcessingTime()
                ));
            }

            Dataset<Row> df = spark.createDataFrame(rows, TableHandler.tableSchema());

            long numRecords;
            // serialize destination uploads
            synchronized (uploadLock) {
                tableHandler.writeToTable(df);

                numRecords = df.count();
                logger.debug("Uploaded {} rows in upload time:{}, ",
                        numRecords,
                        Duration.between(start, Instant.now()).truncatedTo(ChronoUnit.SECONDS)
                );
            }
            df.unpersist();
        } catch (Exception e) {
            logger.error("Error uploading data", e);
        }

        return 0;
    }

    private void logConsumerProgress(long numUploadedEvents) {
        numConsumedEvents += numUploadedEvents;
        if (logTimer.expired()) {
            logger.info("Consumed {} records after {}", numConsumedEvents, Strings.duration(clock.currentTimeInMillis() - consumerStart));
            numConsumedEvents = 0;
            consumerStart = clock.currentTimeInMillis();
            logTimer = Threads.timer(clock, LOG_INTERVAL);
        }
    }

    private void stopSparkSession() {
        try {
            logger.info("Closing Spark");
            if (!spark.sparkContext().isStopped()) {
                spark.close();
            }
            logger.debug("Closed Spark");
        } catch (Exception e) {
            logger.warn("Exception during Spark shutdown ", e);
        }
    }

    private SourceRecord extractSourceRecord(ChangeEvent<?, ?> record) {
        try {
            Class<?> clazz = record.getClass();
            Method method = clazz.getDeclaredMethod("sourceRecord");
            method.setAccessible(true);
            return (SourceRecord) method.invoke(record);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            logger.error("Error extracting source record", e);
            throw new RuntimeException(e);
        }
    }

    class CdcEvent {
        private final String sourceServer;
        private final String sourceTopic;
        private final Long sourceOffsetTsSec;
        private final String sourceOffsetFile;
        private final Long sourceOffsetPos;
        private final Boolean sourceOffsetSnapshot;
        private final String key;
        private final String value;
        private final Instant processingTime;

        public CdcEvent(ChangeEvent<Object, Object> record) {
            SourceRecord sourceRecord = extractSourceRecord(record);

            this.sourceTopic = sourceRecord.topic();
            this.sourceServer = (String) sourceRecord.sourcePartition().get("server");
            this.sourceOffsetTsSec = (Long) sourceRecord.sourceOffset().get("ts_sec");
            this.sourceOffsetFile = (String) sourceRecord.sourceOffset().get("file");
            this.sourceOffsetPos = (Long) sourceRecord.sourceOffset().get("pos");
            this.sourceOffsetSnapshot = (Boolean) sourceRecord.sourceOffset().get("snapshot");
            this.key = (String) record.key();
            this.value = (String) record.value();
            this.processingTime = Instant.now();
        }

        public String getSourceServer() {
            return sourceServer;
        }

        public String getSourceTopic() {
            return sourceTopic;
        }

        public Long getSourceOffsetTsSec() {
            return sourceOffsetTsSec;
        }

        public String getSourceOffsetFile() {
            return sourceOffsetFile;
        }

        public Long getSourceOffsetPos() {
            return sourceOffsetPos;
        }

        public Boolean getSourceOffsetSnapshot() {
            return sourceOffsetSnapshot;
        }

        public String getKey() {
            return key;
        }

        public String getValue() {
            return value;
        }

        public Instant getProcessingTime() {
            return processingTime;
        }
    }
}