package io.debezium.server.iomete;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.Dependent;
import javax.inject.Inject;
import javax.inject.Named;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.col;

@Named("iomete")
@Dependent
public class IometeChangeConsumer extends AbstractChangeConsumer {
    @ConfigProperty(name = "debezium.sink.iomete.cast-deleted-field", defaultValue = "false")
    Boolean castDeletedField;
    @Inject
    protected SparkSession spark;
    @Inject
    TableHandler tableHandler;

    public void initizalize() throws InterruptedException {
        super.initizalize();
    }

    @PostConstruct
    void connect() throws InterruptedException {
        this.initizalize();
    }

    @PreDestroy
    void close() {
        this.stopSparkSession();
    }


    private String writeJsonNodeAsString(JsonNode e) {
        try {
            return mapper.writeValueAsString(e);
        } catch (JsonProcessingException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public long uploadDestination(String destination, List<DebeziumSparkEvent> data) {
        try {
            Instant start = Instant.now();

            DebeziumSparkEvent sampleEvent = data.get(0);

            StructType newEventsSchema = data.get(0).getSparkDfSchema();
            List<String> dataList = data.stream()
                    .map(e -> writeJsonNodeAsString(e.value()))
                    .collect(Collectors.toList());
            Dataset<String> ds = spark.createDataset(dataList, Encoders.STRING());
            Dataset<Row> df;

            if (newEventsSchema != null) {
                LOGGER.debug("Reading data with schema definition, Schema:\n{}", newEventsSchema);
                df = spark.read().schema(newEventsSchema).json(ds);
            } else {
                LOGGER.debug("Reading data without schema definition");
                df = spark.read().json(ds);
            }

            if (castDeletedField) {
                df = df.withColumn("__deleted", col("__deleted").cast(DataTypes.BooleanType));
            }

            long numRecords;

            // serialize same destination uploads
            synchronized (uploadLock.computeIfAbsent(destination, k -> new Object())) {
                final var sourceSchema = df.schema();
                var table =
                        tableHandler.loadTableByDestination(destination)
                                .orElseGet(() -> tableHandler.createTable(
                                        destination, sampleEvent.keyColumnNames(), sourceSchema));

                tableHandler.writeToTable(table, df);

                numRecords = df.count();
                LOGGER.debug("Uploaded {} rows to:'{}' upload time:{}, ",
                        numRecords,
                        table.name,
                        Duration.between(start, Instant.now()).truncatedTo(ChronoUnit.SECONDS)
                );
            }

            if (LOGGER.isTraceEnabled()) {
                df.toJavaRDD().foreach(x ->
                        LOGGER.trace("uploadDestination df row val:{}", x.toString())
                );
            }
            df.unpersist();
            return numRecords;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return 0;
    }

    protected void stopSparkSession() {
        try {
            LOGGER.info("Closing Spark");
            if (!spark.sparkContext().isStopped()) {
                spark.close();
            }
            LOGGER.debug("Closed Spark");
        } catch (Exception e) {
            LOGGER.warn("Exception during Spark shutdown ", e);
        }
    }
}