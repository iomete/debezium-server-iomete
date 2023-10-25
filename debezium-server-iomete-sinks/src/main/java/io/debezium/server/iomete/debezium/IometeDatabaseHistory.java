package io.debezium.server.iomete.debezium;

import io.debezium.annotation.ThreadSafe;
import io.debezium.config.Configuration;
import io.debezium.relational.history.AbstractDatabaseHistory;
import io.debezium.relational.history.DatabaseHistoryListener;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.util.FunctionalReadWriteLock;

import java.util.function.Consumer;

@ThreadSafe
public final class IometeDatabaseHistory extends AbstractDatabaseHistory {
    private final FunctionalReadWriteLock lock = FunctionalReadWriteLock.reentrant();
    LakehouseBackedStateStore lakehouseBackedStateStore = LakehouseBackedStateStoreProvider.instance();

    @Override
    public void configure(
            Configuration config,
            HistoryRecordComparator comparator,
            DatabaseHistoryListener listener,
            boolean useCatalogBeforeSchema) {
        super.configure(config, comparator, listener, useCatalogBeforeSchema);
    }

    @Override
    public void start() {
        super.start();
    }

    @Override
    protected void storeRecord(HistoryRecord record) {
        lock.write(() -> lakehouseBackedStateStore.storeHistoryRecord(record));
    }

    @Override
    protected void recoverRecords(Consumer<HistoryRecord> records) {
        lock.write(() -> lakehouseBackedStateStore.historyRecords().forEach(records));
    }

    @Override
    public boolean storageExists() {
        return true;
    }

    @Override
    public boolean exists() {
        return !lakehouseBackedStateStore.historyRecords().isEmpty();
    }

    @Override
    public String toString() {
        return "IometeDatabaseHistory";
    }
}
