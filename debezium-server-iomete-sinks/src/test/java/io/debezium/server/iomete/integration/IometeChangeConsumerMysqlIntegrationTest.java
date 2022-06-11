package io.debezium.server.iomete.integration;

import io.debezium.server.iomete.shared.SourceMysqlDB;
import io.debezium.util.Testing;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import org.junit.jupiter.api.Test;

import static io.debezium.server.iomete.shared.SparkUtil.await;

@QuarkusTest
@QuarkusTestResource(value = SourceMysqlDB.class, restrictToAnnotatedClass = true)
@TestProfile(IometeChangeConsumerMysqlTestIntegrationProfile.class)
public class IometeChangeConsumerMysqlIntegrationTest {
  @Test
  public void testTombstoneEvents() throws Exception {
    // create test table
    String sqlCreate = "CREATE TABLE IF NOT EXISTS inventory.test_delete_table (" +
        " c_id INTEGER ," +
        " c_id2 INTEGER ," +
        " c_data TEXT," +
        "  PRIMARY KEY (c_id, c_id2)" +
        " );";
    String sqlInsert =
        "INSERT INTO inventory.test_delete_table (c_id, c_id2, c_data ) " +
            "VALUES  (1,1,'data'),(1,2,'data'),(1,3,'data'),(1,4,'data') ;";
    String sqlDelete = "DELETE FROM inventory.test_delete_table where c_id = 1 ;";

    SourceMysqlDB.runSQL(sqlCreate);
    SourceMysqlDB.runSQL(sqlInsert);
    SourceMysqlDB.runSQL(sqlDelete);
    SourceMysqlDB.runSQL(sqlInsert);

    await("default.inventory_test_delete_table", df -> df.count() >= 12);
  }

  @Test
  public void testSimpleUpload() {
    Testing.Print.enable();

    await("default.inventory_customers",
            df -> df.filter("id is not null").count() >= 4);
  }

}
