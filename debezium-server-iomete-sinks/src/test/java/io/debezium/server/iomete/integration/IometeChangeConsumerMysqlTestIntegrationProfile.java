package io.debezium.server.iomete.integration;

import io.quarkus.test.junit.QuarkusTestProfile;

import java.util.HashMap;
import java.util.Map;

public class IometeChangeConsumerMysqlTestIntegrationProfile implements QuarkusTestProfile {

  @Override
  public Map<String, String> getConfigOverrides() {
    Map<String, String> config = new HashMap<>();
    config.put("quarkus.profile", "mysql");
    config.put("%mysql.debezium.source.connector.class", "io.debezium.connector.mysql.MySqlConnector");
    config.put("debezium.source.database.history", "io.debezium.relational.history.MemoryDatabaseHistory");

    config.put("debezium.source.max.batch.size", "500");
    config.put("debezium.source.max.queue.size", "70000");
    config.put("debezium.source.poll.interval.ms", "1000");

    config.put("%mysql.debezium.source.database.include.list", "inventory");

    return config;
  }

  @Override
  public String getConfigProfile() {
    return "mysql";
  }

}
