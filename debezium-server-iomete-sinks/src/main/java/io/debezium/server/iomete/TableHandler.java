package io.debezium.server.iomete;

import io.debezium.DebeziumException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import javax.enterprise.context.Dependent;

import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;

@Dependent
public class TableHandler {
    public static final String PARTITION_COLUMN_NAME = "processing_time";

    private final SparkSession spark;
    private final Configuration configuration;

    public TableHandler(SparkSession spark,
                        Configuration configuration) {
        this.spark = spark;
        this.configuration = configuration;
    }

    public static StructType tableSchema() {
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("source_server", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("source_topic", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("source_offset_ts_sec", DataTypes.LongType, true));
        fields.add(DataTypes.createStructField("source_offset_file", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("source_offset_pos", DataTypes.LongType, true));
        fields.add(DataTypes.createStructField("source_offset_snapshot", DataTypes.BooleanType, true));
        fields.add(DataTypes.createStructField("timestamp", DataTypes.TimestampType, true));
        fields.add(DataTypes.createStructField("key", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("value", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("processing_time", DataTypes.TimestampType, true));

        return DataTypes.createStructType(fields);
    }

    public void createTableIfNotExists() {
        spark.sql(String.format("CREATE TABLE IF NOT EXISTS %s (%s) PARTITIONED BY (days(%s))",
                configuration.fullTableName(), tableSchema().toDDL(), PARTITION_COLUMN_NAME));
    }


    public void writeToTable(Dataset<Row> dataFrame) {
        var tableName = configuration.fullTableName();
        try {
            dataFrame.writeTo(tableName).append();
        } catch (Exception e) {
            throw new DebeziumException(format("Writing to table %s failed", tableName), e);
        }
    }
}
