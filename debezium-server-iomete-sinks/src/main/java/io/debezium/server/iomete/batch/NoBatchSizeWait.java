package io.debezium.server.iomete.batch;

import javax.enterprise.context.Dependent;
import javax.inject.Named;

@Dependent
@Named("NoBatchSizeWait")
public class NoBatchSizeWait implements InterfaceBatchSizeWait {
  public void waitMs(long numRecordsProcessed, Integer processingTimeMs) {
  }
}
