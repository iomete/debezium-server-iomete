package io.debezium.server.iomete.batch;

public interface InterfaceBatchSizeWait {

  default void initizalize() {
  }

  void waitMs(long numRecordsProcessed, Integer processingTimeMs) throws InterruptedException;

}
