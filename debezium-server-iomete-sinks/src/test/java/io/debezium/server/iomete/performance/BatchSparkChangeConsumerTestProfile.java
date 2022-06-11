package io.debezium.server.iomete.performance;

import io.quarkus.test.junit.QuarkusTestProfile;

import java.util.HashMap;
import java.util.Map;

public class BatchSparkChangeConsumerTestProfile implements QuarkusTestProfile {

  @Override
  public Map<String, String> getConfigOverrides() {
    Map<String, String> config = new HashMap<>();

    //https://debezium.io/documentation/reference/stable/connectors/mysql.html
    config.put("debezium.sink.type", "iomete");
    config.put("debezium.source.max.batch.size", "24000");
    config.put("debezium.source.max.queue.size", "700000");
    config.put("debezium.source.table.include.list", "inventory.test_date_table,inventory.customers");
    config.put("quarkus.log.category.\"org.apache.spark.scheduler.TaskSetManager\".level", "ERROR");
    return config;
  }
}
