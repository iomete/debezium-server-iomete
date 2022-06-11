package io.debezium.server.iomete.integration;

import io.quarkus.test.junit.QuarkusTestProfile;

import java.util.HashMap;
import java.util.Map;

public class IometeChangeConsumerIntegrationTestProfile implements QuarkusTestProfile {

  @Override
  public Map<String, String> getConfigOverrides() {
    Map<String, String> config = new HashMap<>();

    config.put("quarkus.log.category.\"io.debezium.server.batch\".level", "INFO");
    return config;
  }
}
