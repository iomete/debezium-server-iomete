package io.debezium.server.iomete.performance;


import java.time.Duration;

import io.debezium.server.iomete.shared.SourcePostgresqlDB;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.awaitility.Awaitility;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.Test;

import static io.debezium.server.iomete.performance.Util.*;
import static io.debezium.server.iomete.shared.SparkUtil.getTableData;

@QuarkusTest
@QuarkusTestResource(value = SourcePostgresqlDB.class, restrictToAnnotatedClass = true)
@TestProfile(BatchSparkChangeConsumerTestProfile.class)
public class BatchSparkChangeConsumerBaseTest {
  @ConfigProperty(name = "debezium.source.max.batch.size", defaultValue = "10003")
  Integer maxBatchSize;

  @Test
  public void testPerformance() throws Exception {
    testPerformance(maxBatchSize);
  }

  public void testPerformance(int maxBatchSize) throws Exception {

    createTestDataTable();

    int iteration = 10;
    for (int i = 0; i <= iteration; i++) {
      new Thread(() -> {
        try {
          loadTestDataTable(maxBatchSize, false);
        } catch (Exception e) {
          e.printStackTrace();
          Thread.currentThread().interrupt();
        }
      }).start();
    }

    Awaitility.await().atMost(Duration.ofSeconds(1200)).until(() -> {
      try {
        Dataset<Row> df = getTableData(TEST_DATE_TABLE_NAME);
        return df.count() >= (long) iteration * maxBatchSize;
      } catch (Exception e) {
        return false;
      }
    });

    Dataset<Row> df = getTableData(TEST_DATE_TABLE_NAME);
    System.out.println("Row Count=" + df.count());
  }

}
