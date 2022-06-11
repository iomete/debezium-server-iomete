package io.debezium.server.iomete;


import org.apache.spark.sql.SparkSession;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Singleton;


@ApplicationScoped
public class SparkProvider {
    @Singleton
    @Produces
    SparkSession sparkSession(){
        return SparkSession.builder()
                .appName("CDC-Batch-Spark-Sink")
                .getOrCreate();
    }
}
