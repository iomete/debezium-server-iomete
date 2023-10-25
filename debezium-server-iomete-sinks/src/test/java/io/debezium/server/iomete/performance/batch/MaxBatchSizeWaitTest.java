package io.debezium.server.iomete.performance.batch;

import io.debezium.server.iomete.DebeziumMetrics;
import io.debezium.server.iomete.shared.SourcePostgresqlDB;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.awaitility.Awaitility;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.Test;

import javax.inject.Inject;
import java.time.Duration;

import static io.debezium.server.iomete.performance.Util.*;
import static io.debezium.server.iomete.shared.SparkUtil.getTableData;

@QuarkusTest
@QuarkusTestResource(value = SourcePostgresqlDB.class, restrictToAnnotatedClass = true)
@TestProfile(MaxBatchSizeWaitTestProfile.class)
class MaxBatchSizeWaitTest {
  @Inject
  DebeziumMetrics debeziumMetrics;
  @ConfigProperty(name = "debezium.source.max.batch.size", defaultValue = "1002")
  Integer maxBatchSize;

  @Test
  public void testPerformance() throws Exception {
    debeziumMetrics.initizalize();
    int iteration = 100;
    createTestDataTable();
    for (int i = 0; i <= iteration; i++) {
      loadTestDataTable(maxBatchSize / 10, true);
    }
    Awaitility.await().atMost(Duration.ofSeconds(120)).until(() -> {
      try {
        Dataset<Row> df = getTableData(String.format("spark_catalog.%s.files", TEST_DATE_TABLE_NAME));
        df.show(false);
        return df.filter("record_count = " + maxBatchSize).count() >= 5;
      } catch (Exception e) {
        return false;
      }
    });
  }
}