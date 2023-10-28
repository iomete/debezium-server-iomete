package io.debezium.server.iomete;

import static java.sql.Timestamp.valueOf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;

class DataWriterTest {

//    private static final String SERVER_NAME = "srv1";
//    Configuration configuration = mock(Configuration.class);
//    PartitionConfiguration partitionConfiguration = mock(PartitionConfiguration.class);
//
//    private final SparkSession spark = SparkTestProvider.sparkSession;
//    TableHandler tableHandler = new TableHandler(spark, mock(DataWriter.class), configuration, partitionConfiguration, SERVER_NAME);
//
//    DataWriter dataWriter = new DataWriter(spark, configuration);
//
//    @BeforeEach
//    public void beforeEachTest() {
//        when(configuration.tableNamespace()).thenReturn("default");
//        when(partitionConfiguration.isDestinationPartitioned(any(), any())).thenReturn(false);
//        //when(configuration.upsertKeepDeletes()).thenReturn(true);
//    }
//
//    private static final StructType SCHEMA = StructType.fromDDL("id bigint, category string, __source_ts timestamp, __source_ts_ms bigint, __op string, __op_num int");
//
//    @Test
//    public void testFirstAppendToTable() {
//        var rows = List.of(
//                row(1L, "c1", tsOfDay(1), 1L, "d", CDC_OPERATIONS.get("d")));
//        var df = spark.createDataFrame(rows, SCHEMA);
//
//        var destination = aDestination();
//        var table = tableHandler.createTable(destination, List.of("id"), SCHEMA);
//
//        dataWriter.appendToTable(table, df);
//
//        var tblDf = spark.table(table.name);
//
//        tblDf.show();
//        assertThat(tblDf.collectAsList()).isEqualTo(List.of(
//                row(1L, "c1", tsOfDay(1), 1L, "d", CDC_OPERATIONS.get("d"))));
//    }
//
//    @Test
//    public void testAppendOutOfOrderSchemaFields() {
//        StructType schema = StructType.fromDDL("category string, id bigint, __source_ts timestamp, __source_ts_ms bigint, __op string, __op_num int");
//
//        var rows = List.of(
//                row(null, 1L, tsOfDay(1), 1L, "d", CDC_OPERATIONS.get("d")));
//        var df = spark.createDataFrame(rows, schema);
//
//        var destination = aDestination();
//        var table = tableHandler.createTable(destination, List.of("id"), SCHEMA);
//
//        dataWriter.appendToTable(table, df);
//
//        var tblDf = spark.table(table.name);
//
//        tblDf.show();
//        assertThat(tblDf.collectAsList()).isEqualTo(List.of(
//                row(1L, null, tsOfDay(1), 1L, "d", CDC_OPERATIONS.get("d"))));
//    }
//
//    @Test
//    public void testMultipleAppendToPartitionedTable() {
//        var rows1 = List.of(
//                row(1L, "c1", tsOfDay(1), 1L, "c", CDC_OPERATIONS.get("c")));
//
//        //create multiple rows and shuffle it
//        var rows2 = IntStream.range(1, 1000)
//                .mapToObj(i -> row((long) i, "c" + i, tsOfDay(i % 29 + 1), (long) i, "c", CDC_OPERATIONS.get("c")))
//                .collect(Collectors.toCollection(ArrayList::new));
//        Collections.shuffle(rows2);
//
//        var df1 = spark.createDataFrame(rows1, SCHEMA);
//        var df2 = spark.createDataFrame(rows2, SCHEMA);
//
//        var destination = aDestination();
//        when(partitionConfiguration.isDestinationPartitioned(destination, SCHEMA)).thenReturn(true);
//        var table = tableHandler.createTable(destination, List.of("id"), SCHEMA);
//
//        assertThat(table.isPartitioned).isTrue();
//
//        dataWriter.appendToTable(table, df1);
//        dataWriter.appendToTable(table, df2);
//
//        var tblDf = spark.table(table.name);
//
//        tblDf.show();
//
//        assertThat(tblDf.collectAsList()).hasSameElementsAs(concat(rows1, rows2));
//    }
//
//    @Test
//    public void testFirstUpsertToTable() {
//        when(configuration.upsertKeepDeletes()).thenReturn(false);
//        var rows = List.of(
//                row(1L, "c1", tsOfDay(1), 1L, "d", CDC_OPERATIONS.get("d")),
//                row(2L, "c2", tsOfDay(2), 2L, "c", CDC_OPERATIONS.get("c")));
//        var df = spark.createDataFrame(rows, SCHEMA);
//
//        var destination = aDestination();
//        var table = tableHandler.createTable(destination, List.of("id"), SCHEMA);
//
//        dataWriter.upsertToTable(table, df);
//
//        var tblDf = spark.table(table.name);
//
//        tblDf.show();
//        //first row is a delete operation. so only return the second row
//        assertThat(tblDf.collectAsList()).isEqualTo(List.of(
//                row(2L, "c2", tsOfDay(2), 2L, "c", CDC_OPERATIONS.get("c"))));
//    }
//
//    @Test
//    public void testUpsertOutOfOrderSchemaFields() {
//        StructType schema = StructType.fromDDL("category string, id bigint, __source_ts timestamp, __source_ts_ms bigint, __op string, __op_num int");
//
//        var rows1 = List.of(
//                row(null, 1L, tsOfDay(1), 1L, "c", CDC_OPERATIONS.get("c")),
//                row(null, 2L, tsOfDay(2), 2L, "c", CDC_OPERATIONS.get("c"))
//        );
//
//        var rows2 = List.of(
//                row("c1_updated", 1L, tsOfDay(1), 1L, "d", CDC_OPERATIONS.get("d")),
//                row("c2_updated", 2L, tsOfDay(2), 2L, "u", CDC_OPERATIONS.get("u")),
//                row("c3_updated", 3L, tsOfDay(3), 3L, "u", CDC_OPERATIONS.get("u"))
//        );
//
//
//        var df1 = spark.createDataFrame(rows1, schema);
//        var df2 = spark.createDataFrame(rows2, schema);
//
//        var destination = aDestination();
//        var table = tableHandler.createTable(destination, List.of("id"), SCHEMA);
//
//        dataWriter.upsertToTable(table, df1);
//        dataWriter.upsertToTable(table, df2);
//
//        var tblDf = spark.table(table.name);
//
//        tblDf.show();
//        assertThat(tblDf.collectAsList()).isEqualTo(List.of(
//                row(1L, "c1_updated", tsOfDay(1), 1L, "d", CDC_OPERATIONS.get("d")),
//                row(2L, "c2_updated", tsOfDay(2), 2L, "u", CDC_OPERATIONS.get("u")),
//                row(3L, "c3_updated", tsOfDay(3), 3L, "u", CDC_OPERATIONS.get("u"))
//        ));
//    }
//
//    @Test
//    public void testMultipleUpsertToPartitionedTable() {
//        var rows1 = List.of(
//                row(1L, "c1", tsOfDay(1), 1L, "c", CDC_OPERATIONS.get("c")),
//                row(2L, "c2", tsOfDay(2), 2L, "c", CDC_OPERATIONS.get("c"))
//        );
//
//        //update 1st batch with the following rows (see 1, 2 rows)
//        var rows2 = List.of(
//                row(1L, "c1_updated", tsOfDay(1), 1L, "d", CDC_OPERATIONS.get("d")),
//                row(2L, "c2_updated", tsOfDay(2), 2L, "u", CDC_OPERATIONS.get("u")),
//                row(3L, "c3_updated", tsOfDay(3), 3L, "u", CDC_OPERATIONS.get("u"))
//        );
//
//        var df1 = spark.createDataFrame(rows1, SCHEMA);
//        var df2 = spark.createDataFrame(rows2, SCHEMA);
//
//        var destination = aDestination();
//        when(partitionConfiguration.isDestinationPartitioned(destination, SCHEMA)).thenReturn(true);
//        var table = tableHandler.createTable(destination, List.of("id"), SCHEMA);
//
//        assertThat(table.isPartitioned).isTrue();
//
//        dataWriter.upsertToTable(table, df1);
//        dataWriter.upsertToTable(table, df2);
//
//        var tblDf = spark.table(table.name);
//
//        tblDf.show();
//
//        var expectedRows = List.of(
//                row(1L, "c1_updated", tsOfDay(1), 1L, "d", CDC_OPERATIONS.get("d")),
//                row(2L, "c2_updated", tsOfDay(2), 2L, "u", CDC_OPERATIONS.get("u")),
//                row(3L, "c3_updated", tsOfDay(3), 3L, "u", CDC_OPERATIONS.get("u")));
//
//        assertThat(tblDf.collectAsList()).hasSameElementsAs(expectedRows);
//    }
//
//    @Test
//    public void testMultipleUpsertToPartitionedTableDeleteRecords() {
//        when(configuration.upsertKeepDeletes()).thenReturn(false);
//
//        var rows1 = List.of(
//                row(1L, "c1", tsOfDay(1), 1L, "c", CDC_OPERATIONS.get("c")),
//                row(2L, "c2", tsOfDay(2), 2L, "c", CDC_OPERATIONS.get("c"))
//        );
//
//        //update 1st batch with the following rows (see 1, 2 rows)
//        var rows2 = List.of(
//                row(1L, "c1_updated", tsOfDay(1), 1L, "d", CDC_OPERATIONS.get("d")),
//                row(2L, "c2_updated", tsOfDay(2), 2L, "u", CDC_OPERATIONS.get("u")),
//                row(3L, "c3_updated", tsOfDay(3), 3L, "u", CDC_OPERATIONS.get("u"))
//        );
//
//        var df1 = spark.createDataFrame(rows1, SCHEMA);
//        var df2 = spark.createDataFrame(rows2, SCHEMA);
//
//        var destination = aDestination();
//        when(partitionConfiguration.isDestinationPartitioned(destination, SCHEMA)).thenReturn(true);
//        var table = tableHandler.createTable(destination, List.of("id"), SCHEMA);
//
//        assertThat(table.isPartitioned).isTrue();
//
//        dataWriter.upsertToTable(table, df1);
//        dataWriter.upsertToTable(table, df2);
//
//        var tblDf = spark.table(table.name);
//
//        tblDf.show();
//
//
//        var expectedRows = List.of(
//                row(2L, "c2_updated", tsOfDay(2), 2L, "u", CDC_OPERATIONS.get("u")),
//                row(3L, "c3_updated", tsOfDay(3), 3L, "u", CDC_OPERATIONS.get("u")));
//
//        assertThat(tblDf.collectAsList()).hasSameElementsAs(expectedRows);
//    }
//
//    @Test
//    public void testUpsertModeDedupWithinBatch() {
//        var rows1 = List.of(
//                row(1L, "c1", tsOfDay(1), 1L, "c", CDC_OPERATIONS.get("c")),
//                row(2L, "c2", tsOfDay(1), 1L, "c", CDC_OPERATIONS.get("c")),
//                row(3L, "c3", tsOfDay(1), 1L, "c", CDC_OPERATIONS.get("c")),
//                row(4L, "c4", tsOfDay(1), 1L, "c", CDC_OPERATIONS.get("c"))
//        );
//
//        //update 1st batch with the following rows (see 1, 2 rows)
//        var rows2 = List.of(
//                // out of order duplicates. 'c1_updated2' will be used which has higher ts
//                row(1L, "c1_updated2", tsOfDay(1), 2L, "u", CDC_OPERATIONS.get("u")),
//                row(1L, "c1_updated1", tsOfDay(1), 1L, "u", CDC_OPERATIONS.get("u")),
//
//                // out of order duplicates. 'c1_updated2' will be used. higher cdc_op (d>u)
//                row(2L, "c2_updated2", tsOfDay(1), 1L, "u", CDC_OPERATIONS.get("d")),
//                row(2L, "c2_updated1", tsOfDay(1), 1L, "u", CDC_OPERATIONS.get("u")),
//
//                //no duplicate
//                row(3L, "c3_updated1", tsOfDay(1), 1L, "u", CDC_OPERATIONS.get("u"))
//        );
//
//        var df1 = spark.createDataFrame(rows1, SCHEMA);
//        var df2 = spark.createDataFrame(rows2, SCHEMA);
//
//        var destination = aDestination();
//        when(partitionConfiguration.isDestinationPartitioned(destination, SCHEMA)).thenReturn(true);
//        var table = tableHandler.createTable(destination, List.of("id"), SCHEMA);
//
//        assertThat(table.isPartitioned).isTrue();
//
//        dataWriter.upsertToTable(table, df1);
//        dataWriter.upsertToTable(table, df2);
//
//        var tblDf = spark.table(table.name);
//
//        tblDf.show();
//
//
//        var expectedRows = List.of(
//                row(4L, "c4", tsOfDay(1), 1L, "c", CDC_OPERATIONS.get("c")),
//
//                //upserted ones
//                row(1L, "c1_updated2", tsOfDay(1), 2L, "u", CDC_OPERATIONS.get("u")),
//                row(2L, "c2_updated2", tsOfDay(1), 1L, "u", CDC_OPERATIONS.get("d")),
//                row(3L, "c3_updated1", tsOfDay(1), 1L, "u", CDC_OPERATIONS.get("u"))
//        );
//
//        assertThat(tblDf.collectAsList()).hasSameElementsAs(expectedRows);
//    }
//
//    @Test
//    public void testUpsertModeDedupInTable() {
//        var rows1 = List.of(
//                row(1L, "c1", tsOfDay(1), 1L, "c", CDC_OPERATIONS.get("u")),
//                row(2L, "c2", tsOfDay(2), 2L, "u", CDC_OPERATIONS.get("u")),
//                row(3L, "c3", tsOfDay(3), 3L, "u", CDC_OPERATIONS.get("u")),
//                row(4L, "c4", tsOfDay(4), 4L, "u", CDC_OPERATIONS.get("u"))
//        );
//
//        //update 1st batch with the following rows (see 1, 2 rows)
//        var rows2 = List.of(
//                //this row won't be updated. The row(id=1) ts (0 < 1) is lower than the existing one
//                row(1L, "c1_updated", tsOfDay(1), 0L, "u", CDC_OPERATIONS.get("u")),
//
//                //this row won't be updated. The row(id=2) have same ts (2=2) and cdc_op (c<u) is lower than the existing one
//                row(2L, "c2_updated", tsOfDay(2), 2L, "c", CDC_OPERATIONS.get("c")),
//
//                //this row will be updated. Because it has higher ts(4>3)
//                row(3L, "c3_updated", tsOfDay(3), 4L, "u", CDC_OPERATIONS.get("u")),
//
//                //this row will be updated. Even though it has same ts (4=4), it has higher cdc_op(4>3)
//                row(4L, "c4_updated", tsOfDay(4), 4L, "d", CDC_OPERATIONS.get("d"))
//        );
//
//        var df1 = spark.createDataFrame(rows1, SCHEMA);
//        var df2 = spark.createDataFrame(rows2, SCHEMA);
//
//        var destination = aDestination();
//        when(partitionConfiguration.isDestinationPartitioned(destination, SCHEMA)).thenReturn(true);
//        var table = tableHandler.createTable(destination, List.of("id"), SCHEMA);
//
//        assertThat(table.isPartitioned).isTrue();
//
//        dataWriter.upsertToTable(table, df1);
//        dataWriter.upsertToTable(table, df2);
//
//        var tblDf = spark.table(table.name);
//
//        tblDf.show();
//
//
//        var expectedRows = List.of(
//                row(1L, "c1", tsOfDay(1), 1L, "c", CDC_OPERATIONS.get("u")),
//                row(2L, "c2", tsOfDay(2), 2L, "u", CDC_OPERATIONS.get("u")),
//
//                row(3L, "c3_updated", tsOfDay(3), 4L, "u", CDC_OPERATIONS.get("u")),
//                row(4L, "c4_updated", tsOfDay(4), 4L, "d", CDC_OPERATIONS.get("d"))
//        );
//
//        assertThat(tblDf.collectAsList()).hasSameElementsAs(expectedRows);
//    }
//
//    @Test
//    public void testUpsertModeDedupInTableDeleteEnabled() {
//        when(configuration.upsertKeepDeletes()).thenReturn(false);
//
//        var rows1 = List.of(
//                row(1L, "c1", tsOfDay(1), 1L, "c", CDC_OPERATIONS.get("u")),
//                row(2L, "c2", tsOfDay(2), 2L, "u", CDC_OPERATIONS.get("u")),
//                row(3L, "c3", tsOfDay(3), 3L, "u", CDC_OPERATIONS.get("u")),
//                row(4L, "c4", tsOfDay(4), 4L, "u", CDC_OPERATIONS.get("u"))
//        );
//
//        //update 1st batch with the following rows (see 1, 2 rows)
//        var rows2 = List.of(
//                //this row won't be updated. The row(id=1) ts (0 < 1) is lower than the existing one
//                row(1L, "c1_updated", tsOfDay(1), 0L, "u", CDC_OPERATIONS.get("u")),
//
//                //this row won't be updated. The row(id=2) have same ts (2=2) and cdc_op (c<u) is lower than the existing one
//                row(2L, "c2_updated", tsOfDay(2), 2L, "c", CDC_OPERATIONS.get("c")),
//
//                //this row will be updated. Because it has higher ts(4>3)
//                row(3L, "c3_updated", tsOfDay(3), 4L, "u", CDC_OPERATIONS.get("u")),
//
//                //this row will be updated. Even though it has same ts (4=4), it has higher cdc_op(4>3)
//                row(4L, "c4_updated", tsOfDay(4), 4L, "d", CDC_OPERATIONS.get("d"))
//        );
//
//        var df1 = spark.createDataFrame(rows1, SCHEMA);
//        var df2 = spark.createDataFrame(rows2, SCHEMA);
//
//        var destination = aDestination();
//        when(partitionConfiguration.isDestinationPartitioned(destination, SCHEMA)).thenReturn(true);
//        var table = tableHandler.createTable(destination, List.of("id"), SCHEMA);
//
//        assertThat(table.isPartitioned).isTrue();
//
//        dataWriter.upsertToTable(table, df1);
//        dataWriter.upsertToTable(table, df2);
//
//        var tblDf = spark.table(table.name);
//
//        tblDf.show();
//
//
//        var expectedRows = List.of(
//                row(1L, "c1", tsOfDay(1), 1L, "c", CDC_OPERATIONS.get("u")),
//                row(2L, "c2", tsOfDay(2), 2L, "u", CDC_OPERATIONS.get("u")),
//
//                row(3L, "c3_updated", tsOfDay(3), 4L, "u", CDC_OPERATIONS.get("u"))
//        );
//
//        assertThat(tblDf.collectAsList()).hasSameElementsAs(expectedRows);
//    }
//
//    private Timestamp tsOfDay(int day) {
//        return valueOf(String.format("2020-06-%02d 22:41:30", day));
//    }
//
//    private static <T> List<T> concat(List<T> l1, List<T> l2) {
//        return Stream.concat(l1.stream(), l2.stream()).collect(Collectors.toList());
//    }
//
//    private static Row row(Object... values) {
//        return RowFactory.create(values);
//    }
//
//    private String aDestination() {
//        return String.format("db1.%s", UUID.randomUUID().toString().replace("-", "_"));
//    }
}