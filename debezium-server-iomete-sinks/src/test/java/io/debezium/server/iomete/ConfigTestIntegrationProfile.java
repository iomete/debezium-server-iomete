package io.debezium.server.iomete;

import io.quarkus.test.junit.QuarkusTestProfile;

import java.util.HashMap;
import java.util.Map;

public class ConfigTestIntegrationProfile implements QuarkusTestProfile {

  @Override
  public Map<String, String> getConfigOverrides() {
    Map<String, String> config = new HashMap<>();
    config.put("debezium.foo3", "bar");
    return config;
  }

  @Override
  public String getConfigProfile() {
    return "mysql";
  }

}
