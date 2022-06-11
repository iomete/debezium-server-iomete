package io.debezium.server.iomete.shared;

import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.awaitility.Awaitility;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.function.Predicate;

public class SparkUtil {

    public static void await(String table, Predicate<Dataset<Row>> predicate) {
        Awaitility.await().atMost(Duration.ofSeconds(60)).until(() -> {
            try {
                Dataset<Row> df = getTableData(table);
                df.show(false);
                return predicate.test(df);
            } catch (Exception e) {
                return false;
            }
        });
    }

    public static Dataset<Row> getTableData(String table) {
        return SparkTestProvider.sparkSession.table(table);
    }
}
