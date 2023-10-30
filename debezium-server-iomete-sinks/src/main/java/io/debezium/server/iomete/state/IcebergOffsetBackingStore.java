package io.debezium.server.iomete.state;

import org.apache.kafka.common.utils.ThreadUtils;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.util.Callback;

import javax.enterprise.context.Dependent;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@Dependent
public class IcebergOffsetBackingStore implements org.apache.kafka.connect.storage.OffsetBackingStore {
    LakehouseBackedStateStore lakehouseBackedStateStore = LakehouseBackedStateStoreProvider.instance();
    protected ExecutorService executor;

    public IcebergOffsetBackingStore() {
    }

    @Override
    public void start() {
        executor = Executors.newFixedThreadPool(1, ThreadUtils.createThreadFactory(
                this.getClass().getSimpleName() + "-%d", false));
    }

    @Override
    public void stop() {
        if (executor != null) {
            executor.shutdown();
            // Best effort wait for any get() and set() tasks (and caller's callbacks) to complete.
            try {
                executor.awaitTermination(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            if (!executor.shutdownNow().isEmpty()) {
                throw new ConnectException("Failed to stop IcebergOffsetBackingStore. Exiting without cleanly " +
                        "shutting down pending tasks and/or callbacks.");
            }
            executor = null;
        }
    }

    @Override
    public Future<Void> set(Map<ByteBuffer, ByteBuffer> values, Callback<Void> callback) {
        return executor.submit(() -> {
            lakehouseBackedStateStore.setOffsets(values);
            lakehouseBackedStateStore.takeSnapshot();
            if (callback != null)
                callback.onCompletion(null, null);
            return null;
        });
    }

    @Override
    public Future<Map<ByteBuffer, ByteBuffer>> get(Collection<ByteBuffer> keys) {
        return executor.submit(() -> lakehouseBackedStateStore.getOffsets(keys));
    }

    @Override
    public void configure(WorkerConfig config) {}
}
