package io.debezium.server.iomete.batch;

public interface InterfaceBatchSizeWait {
  default void init() {}
  void waitMs(long numRecordsProcessed, Integer processingTimeMs) throws InterruptedException;
}
