package io.debezium.server.iomete.debezium;

import io.debezium.relational.history.HistoryRecord;
import io.debezium.server.iomete.shared.SparkTestProvider;
//import io.debezium.server.iomete.state.DatabaseHistoryState;
import io.debezium.server.iomete.state.LakehouseBackedStateStore;
//import io.debezium.server.iomete.state.OffsetState;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class LakehouseBackedStateStoreTest {
//
//    OffsetState offsetState = mock(OffsetState.class);
//    DatabaseHistoryState databaseHistoryState = mock(DatabaseHistoryState.class);
//
//    SparkSession spark = SparkTestProvider.sparkSession;
//
//    LakehouseBackedStateStore lakehouseBackedStateStore = new LakehouseBackedStateStore(spark,
//            offsetState, databaseHistoryState);
//
//    HistoryRecord historyRecord = mock(HistoryRecord.class);
//
//    @Test
//    public void verifyStoreHistoryRecord() {
//        lakehouseBackedStateStore.storeHistoryRecord(historyRecord);
//        verify(databaseHistoryState).storeHistoryRecord(historyRecord);
//    }
//
//    @Test
//    public void verifyHistoryRecord() {
//        lakehouseBackedStateStore.historyRecords();
//        verify(databaseHistoryState).historyRecords();
//    }
//
//    @Test
//    public void verifySetOffsets() {
//        lakehouseBackedStateStore.setOffsets(Map.of());
//        verify(offsetState).setOffsets(Map.of());
//    }
//
//    @Test
//    public void verifyGetOffsets() {
//        Collection<ByteBuffer> keys = List.of();
//        lakehouseBackedStateStore.getOffsets(keys);
//        verify(offsetState).getOffsets(keys);
//    }
//
//    @Test
//    public void testTakeSnapshot() throws IOException {
//        var offsetsData = "offsetsData";
//        var databaseHistoryData = "databaseHistoryState";
//
//        when(offsetState.getState()).thenReturn(offsetsData);
//        when(databaseHistoryState.getState()).thenReturn(databaseHistoryData);
//
//        lakehouseBackedStateStore.takeSnapshot();
//
//        var df = spark.table("default.cdc_state");
//        assertThat(df.count()).isEqualTo(1);
//
//        var row = df.collectAsList().get(0);
//        assertThat(row.getString(0)).isEqualTo(offsetsData);
//        assertThat(row.getString(1)).isEqualTo(databaseHistoryData);
//
//    }

}