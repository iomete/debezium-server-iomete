package io.debezium.server.iomete;

import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PartitionConfigurationTest {

    private static final String SERVER_NAME = "srv1";

    Configuration configuration = mock(Configuration.class);
    PartitionConfiguration partitionConfiguration = new PartitionConfiguration(configuration, SERVER_NAME);

    @Test
    public void testNoTablePartitionConfigShouldReturnFalse() {
        isDestinationPartitioned("db1.tbl1", schema(), Set.of(), false);
    }

    @Test
    public void testTablePartitionConfigIncludeDestinationShouldReturnTrue() {
        isDestinationPartitioned("db1.tbl1", schema(), Set.of("db1.tbl1"), true);
    }

    @Test
    public void testTablePartitionConfigIncludeDestinationButNoSourceTsColumnInSchemaShouldReturnTrue() {
        isDestinationPartitioned("db1.tbl1", schemaWithoutSourceTs(), Set.of("db1.tbl1"), false);
    }

    @Test
    public void testDestinationNameWithServerShouldMatchWithoutServerPrefix() {
        isDestinationPartitioned("srv1.db1.tbl1", schema(), Set.of("db1.tbl1"), true);
    }

    public void isDestinationPartitioned(
            String destination, StructType schema, Set<String> partitionConfig, boolean expectedResult) {
        when(configuration.partitionsConfig()).thenReturn(partitionConfig);

        assertThat(partitionConfiguration.isDestinationPartitioned(destination, schema))
                .isEqualTo(expectedResult);
    }

    private StructType schema() {
        return StructType.fromDDL("id bigint, category string, __source_ts timestamp");
    }

    private StructType schemaWithoutSourceTs() {
        return StructType.fromDDL("id bigint, category string");
    }

}