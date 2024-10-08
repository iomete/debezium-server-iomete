package io.debezium.server.iomete;

import io.debezium.server.TestConfigSource;

public class IometeTestConfigSource extends TestConfigSource {

  public IometeTestConfigSource() {

    config.put("quarkus.profile", "postgresql");

    // DEBEZIUM SOURCE conf
    config.put("debezium.source.connector.class", "io.debezium.connector.postgresql.PostgresConnector");
    config.put("debezium.source.include.schema.changes", "false");
    config.put("debezium.source.decimal.handling.mode", "double");
    config.put("debezium.source.max.batch.size", "100");
    config.put("debezium.source.poll.interval.ms", "5000");

    config.put("debezium.source.database.server.name", "testc");

    config.put("%postgresql.debezium.source.schema.include.list", "inventory");
    config.put("debezium.source.table.include.list", "inventory.customers,inventory.orders,inventory.products," +
        "inventory.products_on_hand,inventory.geom," +
        "inventory.test_date_table,inventory.table_datatypes,inventory.test_delete_table");
    config.put("debezium.source.snapshot.select.statement.overrides.inventory.products_on_hand", "SELECT * FROM products_on_hand WHERE 1>2");
    // enable disable schema
    config.put("debezium.format.value.schemas.enable", "true");

    // common sink conf
    config.put("debezium.sink.type", "iomete");
    config.put("debezium.sink.batch.objectkey-prefix", "debezium-cdc-");
    config.put("debezium.sink.batch.objectkey-partition", "true");

    // debezium unwrap message
    config.put("debezium.transforms", "unwrap");
    config.put("debezium.transforms.unwrap.type", "io.debezium.transforms.ExtractNewRecordState");
    config.put("debezium.transforms.unwrap.add.fields", "op,table,source.ts_ms,db,source.lsn,source.txId");
    config.put("%mysql.debezium.transforms.unwrap.add.fields", "op,table,source.ts_ms,db,source.file,source.pos,source.row,source.gtid");
    config.put("%mysql.debezium.source.internal.implementation", "legacy");
    config.put("debezium.transforms.unwrap.delete.handling.mode", "rewrite");
    config.put("debezium.transforms.unwrap.drop.tombstones", "true");

    // logging levels
    config.put("quarkus.log.level", "INFO");
    config.put("quarkus.log.category.\"org.apache.spark\".level", "DEBUG");
    config.put("quarkus.log.category.\"org.apache.hadoop\".level", "ERROR");
    config.put("quarkus.log.category.\"org.apache.parquet\".level", "WARN");
    config.put("quarkus.log.category.\"org.eclipse.jetty\".level", "WARN");
    config.put("quarkus.log.category.\"io.debezium.server.batch.spark\".level", "INFO");

  }

  @Override
  public int getOrdinal() {
    // Configuration property precedence is based on ordinal values and since we override the
    // properties in TestConfigSource, we should give this a higher priority.
    return Integer.MAX_VALUE;
  }
}
