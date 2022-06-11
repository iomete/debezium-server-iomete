package io.debezium.server.iomete;

import org.apache.spark.sql.types.StructType;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.enterprise.context.Dependent;
import java.util.Arrays;

@Dependent
public class PartitionConfiguration {
    public static String PARTITION_COLUMN_NAME = "__source_ts";

    private final Configuration configuration;
    private final String sourceDatabaseServerNamePrefix;

    public PartitionConfiguration(Configuration configuration,
                                  @ConfigProperty(name = "debezium.source.database.server.name")
                                  String sourceDatabaseServerName) {
        this.configuration = configuration;
        this.sourceDatabaseServerNamePrefix = String.format("%s.", sourceDatabaseServerName);
    }

    public boolean isDestinationPartitioned(String destination, StructType schema) {
        String cleanDestination = destination;
        if (destination.startsWith(sourceDatabaseServerNamePrefix)) {
            cleanDestination = destination.substring(sourceDatabaseServerNamePrefix.length());
        }

        return configuration.partitionsConfig().contains(cleanDestination) &&
                Arrays.asList(schema.fieldNames()).contains("__source_ts");
    }
}
