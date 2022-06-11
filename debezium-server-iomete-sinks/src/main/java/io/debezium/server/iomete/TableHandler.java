package io.debezium.server.iomete;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.collection.immutable.Seq;

import javax.enterprise.context.Dependent;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.debezium.server.iomete.ScalaConversionHelper.mapToSeq;
import static org.apache.spark.sql.functions.lit;

@Dependent
public class TableHandler {
    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private final SparkSession spark;
    private final Configuration configuration;

    private final PartitionConfiguration partitionConfiguration;

    private final String sourceDatabaseServerNamePrefix;
    private final DataWriter dataWriter;

    public TableHandler(SparkSession spark,
                        DataWriter dataWriter,
                        Configuration configuration,
                        PartitionConfiguration partitionConfiguration,
                        @ConfigProperty(name = "debezium.source.database.server.name")
                        String sourceDatabaseServerName) {
        this.spark = spark;
        this.configuration = configuration;
        this.partitionConfiguration = partitionConfiguration;
        this.sourceDatabaseServerNamePrefix = String.format("%s.", sourceDatabaseServerName);
        this.dataWriter = dataWriter;
    }

    public Optional<Table> loadTableByDestination(String destination) {
        var tableName = destinationToTableName(destination);
        return loadTable(tableName);
    }

    public Optional<Table> loadTable(String tableName) {
        try {
            var table = this.spark.table(tableName).filter("1=2");
            var schema = table.schema();

            return Optional.of(new Table(tableName, schema));
        } catch (Exception analysisException) {
            if (!analysisException.getMessage().startsWith("Table or view not found")) {
                logger.error("AnalysisException on load table", analysisException);
            }

            return Optional.empty();
        }
    }

    public Table createTable(
            String destination,
            List<String> keyFieldNames,
            StructType schema) {

        var tableName = destinationToTableName(destination);
        if (partitionConfiguration.isDestinationPartitioned(destination, schema)) {
            spark.sql(String.format("CREATE TABLE %s (%s) PARTITIONED BY (days(%s))",
                    tableName, schema.toDDL(), PartitionConfiguration.PARTITION_COLUMN_NAME));
            if (!keyFieldNames.isEmpty()) {
                spark.sql(String.format("ALTER TABLE spark_catalog.%s WRITE DISTRIBUTED BY PARTITION LOCALLY ORDERED BY %s",
                        tableName, String.join(", ", keyFieldNames)));
            }

            return new Table(tableName, schema, keyFieldNames, true);

        } else {
            spark.sql(String.format("CREATE TABLE %s (%s)", tableName, schema.toDDL()));

            if (!keyFieldNames.isEmpty()) {
                spark.sql(String.format("ALTER TABLE spark_catalog.%s WRITE ORDERED BY %s",
                        tableName, String.join(", ", keyFieldNames)));
            }

            return new Table(tableName, schema, keyFieldNames, false);
        }
    }

    public void writeToTable(Table table, Dataset<Row> sourceDataFrame) {
        var syncResult = syncTableSchema(table, sourceDataFrame);

        var isUpsertMode = configuration.upsert() && !table.keyFields.isEmpty();

        if (configuration.upsert() && table.keyFields.isEmpty()) {
            logger.info("Key fields are not available for table: {}. " +
                    "Thus upsert mode disabled, sync will happen in append mode", table.name);
        }

        if (isUpsertMode) {
            dataWriter.upsertToTable(syncResult._1, syncResult._2);
        } else {
            dataWriter.appendToTable(syncResult._1, syncResult._2);
        }
    }

    public Tuple2<Table, Dataset<Row>> syncTableSchema(Table table, Dataset<Row> sourceDataFrame) {
        var newEventsSchema = sourceDataFrame.schema();
        List<StructField> newFields = getNewFields(table.schema, newEventsSchema);

        boolean shouldReloadTableSchema = false;

        if (!newFields.isEmpty()) {
            logger.info("There are new fields in the new records. Adding those new fields to the table '{}':\n{}",
                    table.name, newFields);

            var columns = newFields.stream()
                    .map(f -> String.format("%s %s", f.name(), f.dataType().simpleString()))
                    .collect(Collectors.toList());

            spark.sql(String.format("ALTER TABLE %s ADD COLUMNS ( %s )",
                    table.name, String.join(", ", columns)));

            //table structure should be changed. reload it
            shouldReloadTableSchema = true;
        }

        List<StructField> deletedFields = getDeletedFields(table.schema, newEventsSchema);
        if (!deletedFields.isEmpty()) {
            Seq<String> deletedFieldNames = mapToSeq(deletedFields, StructField::name);
            Seq<Column> deletedFieldColumns = mapToSeq(deletedFields,
                    f -> lit(null).cast(f.dataType()).as(f.name()));

            sourceDataFrame = sourceDataFrame.withColumns(deletedFieldNames, deletedFieldColumns);
        }

        if (shouldReloadTableSchema) {
            return new Tuple2<>(this.loadTable(table.name).orElseThrow(), sourceDataFrame);
        }
        return new Tuple2<>(table, sourceDataFrame);
    }

    private List<StructField> getNewFields(StructType currentSchema, StructType newSchema) {
        var currentFieldNames = new HashSet<>(Arrays.asList(currentSchema.fieldNames()));
        return Arrays.stream(newSchema.fields())
                .filter(f -> !currentFieldNames.contains(f.name()))
                .collect(Collectors.toList());
    }

    private List<StructField> getDeletedFields(StructType currentSchema, StructType newSchema) {
        var newFieldNames = new HashSet<>(Arrays.asList(newSchema.fieldNames()));
        return Arrays.stream(currentSchema.fields())
                .filter(f -> !newFieldNames.contains(f.name()))
                .collect(Collectors.toList());
    }

    public String destinationToTableName(String destination) {
        String tableName = destination;
        if (tableName.startsWith(sourceDatabaseServerNamePrefix)) {
            tableName = tableName.substring(sourceDatabaseServerNamePrefix.length());
        }

        tableName = tableName
                .replaceAll(this.configuration.destinationRegexp().orElse(""),
                        this.configuration.destinationRegexpReplace().orElse(""))
                .replace(".", "_");

        if (tableName.startsWith(sourceDatabaseServerNamePrefix)) {
            tableName = tableName.substring(sourceDatabaseServerNamePrefix.length());
        }

        return String.format("%s.%s%s",
                this.configuration.tableNamespace(), configuration.tablePrefix().orElse(""), tableName);
    }


    static class Table {
        String name;
        StructType schema;
        boolean isPartitioned = false;

        List<String> keyFields = List.of();

        public Table(String name, StructType schema) {
            this.name = name;
            this.schema = schema;
        }

        public Table(String name, StructType schema, List<String> keyFields, boolean isPartitioned) {
            this(name, schema);
            this.isPartitioned = isPartitioned;
            this.keyFields = keyFields;
        }
    }
}
