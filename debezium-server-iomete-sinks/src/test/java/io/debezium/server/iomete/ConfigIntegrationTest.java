package io.debezium.server.iomete;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(ConfigTestIntegrationProfile.class)
public class ConfigIntegrationTest {
    @ConfigProperty(name = "debezium.source.max.batch.size", defaultValue = "10001")
    Integer maxBatchSize;

    @ConfigProperty(name = "debezium.foo", defaultValue = "val")
    String foo;

    @ConfigProperty(name = "debezium.foo2", defaultValue = "val")
    String foo2;

    @ConfigProperty(name = "debezium.foo3", defaultValue = "val")
    String foo3;

    @Test
    public void configsFromMicroprofileConfigFile() {
        assert foo.equals("bar");
        assert foo2.equals("bar2");
    }

    @Test
    public void configsOverriddenByConfigurationProfile() {
        assert foo3.equals("bar3_override");
    }

    @Test
    public void configsFromConfigSource() {
        assert maxBatchSize == 100;
    }
}
