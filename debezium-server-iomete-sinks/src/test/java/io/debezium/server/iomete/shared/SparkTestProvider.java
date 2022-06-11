package io.debezium.server.iomete.shared;


import io.quarkus.arc.Priority;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.SparkSession;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Alternative;
import javax.enterprise.inject.Produces;
import javax.inject.Singleton;
import java.io.File;
import java.io.IOException;


@ApplicationScoped
public class SparkTestProvider {
    private final static String LAKEHOUSE_DIR = "lakehouse";

    static {
        try {
            File dir = new File(LAKEHOUSE_DIR);
            if (dir.exists()) {
                FileUtils.deleteDirectory(dir);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static SparkSession sparkSession = SparkSession.builder()
            .appName("CDC-Batch-Spark-Sink-Test")
            .master("local")
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
            .config("spark.sql.catalog.spark_catalog.type", "hadoop")
            .config("spark.sql.catalog.spark_catalog.warehouse", LAKEHOUSE_DIR)
            .config("spark.sql.warehouse.dir", LAKEHOUSE_DIR)
            .config("spark.sql.legacy.createHiveTableByDefault", "false")
            .config("spark.sql.sources.default", "iceberg")
            .getOrCreate();

    @Alternative
    @Priority(1)
    @Singleton
    @Produces
    SparkSession sparkSession() {
        return sparkSession;
    }
}
