package io.debezium.server.iomete;


import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;

import java.util.Optional;
import java.util.Set;

@ConfigMapping(prefix = "debezium.sink.iomete")
public interface Configuration {
    @WithName("upsert")
    @WithDefault("false")
    Boolean upsert();

    @WithName("upsert-keep-deletes")
    @WithDefault("true")
    Boolean upsertKeepDeletes();

    @WithName("table-namespace")
    @WithDefault("default")
    String tableNamespace();

    @WithName("destination-regexp")
    @WithDefault("")
    Optional<String> destinationRegexp();

    @WithName("destination-regexp-replace")
    @WithDefault("")
    Optional<String> destinationRegexpReplace();

    @WithName("table-prefix")
    @WithDefault("")
    Optional<String> tablePrefix();

    @WithName("table.partition.configs")
    @WithDefault("")
    Set<String> partitionsConfig();
}
