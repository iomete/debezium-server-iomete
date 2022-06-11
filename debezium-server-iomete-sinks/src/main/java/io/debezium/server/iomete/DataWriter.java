package io.debezium.server.iomete;

import io.debezium.DebeziumException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.Dependent;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.lang.String.format;

@Dependent
public class DataWriter {
    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private final SparkSession spark;
    private final Configuration configuration;

    public DataWriter(SparkSession spark,
                      Configuration configuration) {
        this.spark = spark;
        this.configuration = configuration;
    }

    public void appendToTable(TableHandler.Table table, Dataset<Row> df) {
        try {
            if (table.isPartitioned) {
                df.sortWithinPartitions(PartitionConfiguration.PARTITION_COLUMN_NAME)
                        .writeTo(table.name).append();
            } else {
                df.writeTo(table.name).append();
            }
        } catch (Exception e) {
            throw new DebeziumException(format("Writing to table %s failed", table.name), e);
        }
    }

    private static <S, T> List<T> map(Collection<S> collection, Function<S, T> mapper) {
        return collection.stream().map(mapper).collect(Collectors.toList());
    }

    private Dataset<Row> deduplicateUpsertBatch(TableHandler.Table table, Dataset<Row> df){
        // https://stackoverflow.com/questions/38687212/spark-dataframe-drop-duplicates-and-keep-first
        var dedupColumns = table.keyFields;
        return df.orderBy(df.col("__source_ts_ms").desc(), df.col("__op_num").desc())
                .coalesce(1)
                .dropDuplicates(dedupColumns.toArray(new String[0]));
    }

    public void upsertToTable(TableHandler.Table table, Dataset<Row> df) {
        try {
            var tmpTableName = format("_tmp_%s", table.name.replace(".", "_"));
            deduplicateUpsertBatch(table, df)
                    .createOrReplaceTempView(tmpTableName);

            if (table.keyFields.isEmpty()) {
                throw new DebeziumException(format(
                        "Table: %s doesn't have key fields. Upsert cannot be used for this table", table.name));
            }

            var sourceTable = tmpTableName;
            if (table.isPartitioned) {
                // writing to partitioned table requre the source sorted by partition column beforehand
                // see: https://iceberg.apache.org/docs/latest/spark-writes/#writing-to-partitioned-tables
                sourceTable = String.format("(select * from %s order by %s)",
                        tmpTableName, PartitionConfiguration.PARTITION_COLUMN_NAME);
            }

            // like, t.id = s.id and t.level = s.level
            var matchStatement = String.join("AND",
                    map(table.keyFields, f -> format("t.%s=s.%s", f, f)));

            var mergeSql = format(
                    "MERGE INTO %s as t USING %s as s ON %s \n" +
                            "WHEN MATCHED AND " +
                            "   s.__source_ts_ms > t.__source_ts_ms " +
                            "   OR (s.__source_ts_ms = t.__source_ts_ms and s.__op_num > t.__op_num) " +
                            "   THEN UPDATE SET *\n" +
                            "WHEN NOT MATCHED THEN INSERT *",
                    table.name, sourceTable, matchStatement);

            if (!configuration.upsertKeepDeletes()) {
                mergeSql = format(
                        "MERGE INTO %s as t USING %s as s ON %s \n" +
                                "WHEN MATCHED AND s.__op = 'd' THEN DELETE \n" +
                                "WHEN MATCHED AND " +
                                "   s.__source_ts_ms > t.__source_ts_ms " +
                                "   OR (s.__source_ts_ms = t.__source_ts_ms AND s.__op_num > t.__op_num)  " +
                                "   THEN UPDATE SET *\n" +
                                "WHEN NOT MATCHED AND s.__op != 'd' THEN INSERT *",
                        table.name, tmpTableName, matchStatement);
            }

            spark.sql(mergeSql);
        } catch (Exception e) {
            throw new DebeziumException(format("Writing to table %s failed", table.name), e);
        }
    }
}
