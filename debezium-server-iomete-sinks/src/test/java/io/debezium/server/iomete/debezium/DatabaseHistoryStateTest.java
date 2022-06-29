package io.debezium.server.iomete.debezium;

import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.TableChanges;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

class DatabaseHistoryStateTest {
    List<HistoryRecord> records = List.of(
            createHistoryRecord(1),
            createHistoryRecord(2),
            createHistoryRecord(3)
    );

    @Test
    public void getEmptyHistoryRecords() {
        DatabaseHistoryState databaseHistoryState = new DatabaseHistoryState();
        assertThat(databaseHistoryState.historyRecords())
                .isEqualTo(List.of());
    }

    @Test
    public void setAndGetHistoryRecords(){
        DatabaseHistoryState databaseHistoryState = new DatabaseHistoryState();

        records.forEach(databaseHistoryState::storeHistoryRecord);

        assertThat(databaseHistoryState.historyRecords())
                .isEqualTo(records);
    }

    @Test
    public void testSaveAndLoad() throws IOException {
        DatabaseHistoryState databaseHistoryState = new DatabaseHistoryState();
        records.forEach(databaseHistoryState::storeHistoryRecord);

        var state = databaseHistoryState.getState();
        var newDatabaseHistory = new DatabaseHistoryState();
        newDatabaseHistory.loadState(state);

        assertThat(databaseHistoryState.historyRecords().toString())
                .isEqualTo(records.toString());

        assertThat(newDatabaseHistory.historyRecords().toString())
                .isEqualTo(records.toString());

    }

    private HistoryRecord createHistoryRecord(int id) {
        return new HistoryRecord(
                Map.of(format("src%d", id), format("src%d_val", id)),
                Map.of(format("pos%d", id), format("pos%d_val", id)),
                "db" + id,
                "schema" + id,
                "ddl" + id,
                new TableChanges()
        );
    }
}