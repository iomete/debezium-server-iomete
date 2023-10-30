package io.debezium.server.iomete.state;

import io.debezium.annotation.ThreadSafe;
import io.debezium.config.Configuration;
import io.debezium.relational.history.AbstractDatabaseHistory;
import io.debezium.relational.history.DatabaseHistoryListener;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.util.FunctionalReadWriteLock;

import javax.enterprise.context.Dependent;
import java.util.function.Consumer;

@Dependent
@ThreadSafe
public final class IcebergBackedDatabaseHistory extends AbstractDatabaseHistory {
    private final FunctionalReadWriteLock lock = FunctionalReadWriteLock.reentrant();
    LakehouseBackedStateStore lakehouseBackedStateStore = LakehouseBackedStateStoreProvider.instance();

    public IcebergBackedDatabaseHistory(){

    }

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
        return "IcebergBackedDatabaseHistory";
    }
}
