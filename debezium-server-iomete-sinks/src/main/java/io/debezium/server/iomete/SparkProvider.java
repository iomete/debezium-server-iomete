package io.debezium.server.iomete;


import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Singleton;


@ApplicationScoped
public class SparkProvider {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final static String LAKEHOUSE_DIR = "/Users/vusaldadalov/Documents/iomete/github.com/debezium-server-iomete/lakehouseprod";
    @Singleton
    @Produces
    SparkSession sparkSession(){
        logger.debug("Getting spark session");
        return createSparkSession();
    }

    public static SparkSession createSparkSession(){
        return SparkSession.builder()
                .appName("CDC-Batch-Spark-Sink")
                .master("local")
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
                .config("spark.sql.catalog.spark_catalog.type", "hadoop")
                .config("spark.sql.catalog.spark_catalog.warehouse", LAKEHOUSE_DIR)
                .config("spark.sql.warehouse.dir", LAKEHOUSE_DIR)
                .config("spark.sql.legacy.createHiveTableByDefault", "false")
                .config("spark.sql.sources.default", "iceberg")
                .getOrCreate();
    }
}
