package io.debezium.server.iomete;


import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithName;

@ConfigMapping(prefix = "debezium.sink.iomete")
public interface Configuration {
    @WithName("destination.database")
    String destinationDatabase();

    @WithName("destination.table")
    String destinationTable();

    default String fullTableName() {
        return destinationDatabase() + "." + destinationTable();
    }
}
