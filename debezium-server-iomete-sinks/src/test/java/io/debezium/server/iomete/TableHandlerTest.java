package io.debezium.server.iomete;

import io.debezium.server.iomete.shared.SparkTestProvider;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TableHandlerTest {
//    private static final String SERVER_NAME = "srv1";
//    Configuration configuration = mock(Configuration.class);
//    PartitionConfiguration partitionConfiguration = mock(PartitionConfiguration.class);
//
//    DataWriter dataWriter = mock(DataWriter.class);
//
//    private final SparkSession spark = SparkTestProvider.sparkSession;
//
//
//
//    TableHandler tableHandler = new TableHandler(spark, dataWriter, configuration, partitionConfiguration, SERVER_NAME);
//
//    @BeforeEach
//    public void beforeEachTest() {
//        when(configuration.tableNamespace()).thenReturn("default");
//        when(configuration.tablePrefix()).thenReturn(Optional.of(""));
//        when(configuration.destinationRegexp()).thenReturn(Optional.of(""));
//        when(configuration.destinationRegexpReplace()).thenReturn(Optional.of(""));
//
//        when(partitionConfiguration.isDestinationPartitioned(any(), any())).thenReturn(false);
//    }
//
//    @Test
//    public void testLoadAndCreate() {
//        var destination = "db1.tbl1";
//        var expectedTableName = "default.db1_tbl1";
//
//        //first load, no table
//        assertThat(tableHandler.loadTable(expectedTableName)).isEmpty();
//
//        //create table
//        var schema = StructType.fromDDL("id bigint, category string");
//        tableHandler.createTable(destination, List.of(), schema);
//
//        // load again
//        var loadedTable = tableHandler.loadTable(expectedTableName).get();
//
//        assertThat(loadedTable.name).isEqualTo(expectedTableName);
//        assertThat(loadedTable.schema).isEqualTo(schema);
//    }
//
//    @Test
//    public void testCreateNoPartitions() {
//        var destination = "db1.no_partitioned";
//        var tableName = tableHandler.destinationToTableName(destination);
//
//        //create table
//        var schema = StructType.fromDDL("id bigint, category string, __source_ts timestamp");
//        tableHandler.createTable(destination, List.of("id"), schema);
//
//        // load again
//        var loadedTable = tableHandler.loadTable(tableName).get();
//
//        assertThat(loadedTable.name).isEqualTo(tableName);
//        assertThat(loadedTable.schema).isEqualTo(schema);
//
//
//        assertThat(getTableDescriptionField(tableName, "_partition"))
//                .contains("struct<>");
//
//        assertThat(getTableDescriptionField(tableName, "Table Properties"))
//                .contains("sort-order=id ASC NULLS FIRST,write.distribution-mode=range");
//    }
//
//    @Test
//    public void testCreatePartitioned() {
//        var destination = "db1.partitioned";
//        var tableName = tableHandler.destinationToTableName(destination);
//        var schema = StructType.fromDDL("id bigint, category string, __source_ts timestamp");
//
//        when(partitionConfiguration.isDestinationPartitioned(destination, schema)).thenReturn(true);
//
//        //create table
//
//        tableHandler.createTable(destination, List.of("id"), schema);
//
//        // load again
//        var loadedTable = tableHandler.loadTable(tableName).get();
//
//        assertThat(loadedTable.name).isEqualTo(tableName);
//        assertThat(loadedTable.schema).isEqualTo(schema);
//
//        assertThat(getTableDescriptionField(tableName, "_partition"))
//                .contains("struct<__source_ts_day:date>");
//
//        assertThat(getTableDescriptionField(tableName, "Table Properties"))
//                .contains("sort-order=id ASC NULLS FIRST,write.distribution-mode=hash");
//    }
//
//    @Test
//    public void testSyncTableSchemaNoChanges() {
//        var table = new TableHandler.Table(
//                aTableName(), StructType.fromDDL("id bigint, category string, __source_ts timestamp"));
//        spark.sql(String.format("CREATE TABLE %s (%s)", table.name, table.schema.toDDL()));
//
//        var sourceDataset = datasetFromDDL("id bigint, category string, __source_ts timestamp");
//        var resultDs = tableHandler.syncTableSchema(table, sourceDataset);
//
//        assertThat(resultDs._2).isEqualTo(sourceDataset);
//        assertThat(resultDs._1.schema)
//                .isEqualTo(StructType.fromDDL("id bigint, category string, __source_ts timestamp"));
//        assertThat(loadTableSchema(table.name))
//                .isEqualTo(StructType.fromDDL("id bigint, category string, __source_ts timestamp"));
//    }
//
//    @Test
//    public void testSyncTableSchemaNewFields() {
//        var table = new TableHandler.Table(
//                aTableName(), StructType.fromDDL("id bigint"));
//        spark.sql(String.format("CREATE TABLE %s (%s)", table.name, table.schema.toDDL()));
//
//        var sourceDataset = datasetFromDDL("id bigint, category string, __source_ts timestamp");
//        var resultDs = tableHandler.syncTableSchema(table, sourceDataset);
//
//        assertThat(resultDs._2).isEqualTo(sourceDataset);
//        assertThat(resultDs._1.schema).isEqualTo(
//                StructType.fromDDL("id bigint, category string, __source_ts timestamp"));
//        assertThat(loadTableSchema(table.name)).isEqualTo(
//                StructType.fromDDL("id bigint, category string, __source_ts timestamp"));
//    }
//
//    @Test
//    public void testSyncTableSchemaNewAndDeletedFields() {
//        var table = new TableHandler.Table(
//                aTableName(), StructType.fromDDL("id bigint, category string"));
//        spark.sql(String.format("CREATE TABLE %s (%s)", table.name, table.schema.toDDL()));
//
//        var sourceDataset = datasetFromDDL("id bigint, new_field string");
//        var resultDs = tableHandler.syncTableSchema(table, sourceDataset);
//
//        assertThat(resultDs._2.schema())
//                .isEqualTo(StructType.fromDDL("id bigint, new_field string, category string"));
//
//        assertThat(resultDs._1.schema).isEqualTo(
//                StructType.fromDDL("id bigint, category string, new_field string"));
//        assertThat(loadTableSchema(table.name)).isEqualTo(
//                StructType.fromDDL("id bigint, category string, new_field string"));
//    }
//
//    @Test
//    public void testDestinationToTableName(){
//        assertThat(tableHandler.destinationToTableName("db1.tbl_1"))
//                .isEqualTo("default.db1_tbl_1");
//    }
//
//    @Test
//    public void testDestinationToTableNameRemoveServerNamePrefix(){
//        assertThat(tableHandler.destinationToTableName(String.format("%s.db1.tbl_1", SERVER_NAME)))
//                .isEqualTo("default.db1_tbl_1");
//    }
//
//    @Test
//    public void testDestinationToTableNameAllConfigs(){
//        when(configuration.tableNamespace()).thenReturn("cdc");
//        when(configuration.tablePrefix()).thenReturn(Optional.of("prefix_"));
//        when(configuration.destinationRegexp()).thenReturn(Optional.of("\\."));
//        when(configuration.destinationRegexpReplace()).thenReturn(Optional.of("__"));
//
//        assertThat(tableHandler.destinationToTableName(String.format("%s.db1.tbl_1", SERVER_NAME)))
//                .isEqualTo("cdc.prefix_db1__tbl_1");
//    }
//
//    private StructType loadTableSchema(String tableName) {
//        return tableHandler.loadTable(tableName).get().schema;
//    }
//
//    private String aTableName() {
//        return String.format("default.%s", UUID.randomUUID().toString().replace("-", "_"));
//    }
//
//    private Dataset<Row> datasetFromDDL(String ddl) {
//        return spark.createDataFrame(List.of(), StructType.fromDDL(ddl));
//    }
//
//    private String getTableDescriptionField(String tableName, String fieldName) {
//        return spark.sql("describe extended " + tableName).collectAsList()
//                .stream()
//                .filter(row -> row.get(0).equals(fieldName))
//                .findFirst().get().getString(1);
//    }

}