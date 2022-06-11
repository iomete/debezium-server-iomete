package io.debezium.server.iomete.integration;

import io.debezium.server.iomete.shared.SourcePostgresqlDB;
import io.debezium.util.Testing;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.util.function.Predicate;

import static io.debezium.server.iomete.shared.SourcePostgresqlDB.runOnPostgres;
import static io.debezium.server.iomete.shared.SparkUtil.await;

@QuarkusTest
@QuarkusTestResource(value = SourcePostgresqlDB.class, restrictToAnnotatedClass = true)
@TestProfile(IometeChangeConsumerIntegrationTestProfile.class)
public class IometeChangeConsumerIntegrationTest {

    private static final String CUSTOMERS_TABLE = "default.inventory_customers";
    private static final String DATA_TYPES_TABLE = "default.inventory_table_datatypes";

    @Test
    public void testSimpleUpload() {
        Testing.Print.enable();
        awaitCustomers(df -> df.filter("id is not null").count() >= 4);
    }

    @Test
    public void testDatatypes() throws Exception {
        String sql = "\n" +
                "        DROP TABLE IF EXISTS inventory.table_datatypes;\n" +
                "        CREATE TABLE IF NOT EXISTS inventory.table_datatypes (\n" +
                "            c_id INTEGER ,\n" +
                "            c_text TEXT,\n" +
                "            c_varchar VARCHAR,\n" +
                "            c_int INTEGER,\n" +
                "            c_date DATE,\n" +
                "            c_timestamp TIMESTAMP,\n" +
                "            c_timestamptz TIMESTAMPTZ,\n" +
                "            c_float FLOAT,\n" +
                "            c_decimal DECIMAL(18,4),\n" +
                "            c_numeric NUMERIC(18,4),\n" +
                "            c_interval INTERVAL,\n" +
                "            c_boolean BOOLean,\n" +
                "            c_uuid UUID,\n" +
                "            c_bytea BYTEA,\n" +
                "            c_json json,\n" +
                "            c_jsonb jsonb\n" +
                "          );";
        SourcePostgresqlDB.runOnPostgres(sql);
        sql = "INSERT INTO inventory.table_datatypes (" +
                "c_id, " +
                "c_text, c_varchar, c_int, c_date, c_timestamp, c_timestamptz, " +
                "c_float, c_decimal,c_numeric,c_interval,c_boolean,c_uuid,c_bytea,  " +
                "c_json, c_jsonb) " +
                "VALUES (1, null, null, null,null,null,null," +
                "null,null,null,null,null,null,null," +
                "null,null)," +
                "(2, 'val_text', 'A', 123, current_date , current_timestamp, current_timestamp," +
                "'1.23'::float,'1234566.34456'::decimal,'345.452'::numeric(18,4), interval '1 day',false," +
                "'3f207ac6-5dba-11eb-ae93-0242ac130002'::UUID, 'aBC'::bytea," +
                "'{\"reading\": 1123}'::json, '{\"reading\": 1123}'::jsonb" +
                ")";
        SourcePostgresqlDB.runOnPostgres(sql);

        await(DATA_TYPES_TABLE, df -> {
            df = df.withColumn("c_bytea", df.col("c_bytea").cast("string"));
            df = df.withColumn("c_numeric", df.col("c_numeric").cast("float"));
            df = df.withColumn("c_float", df.col("c_float").cast("float"));
            df = df.withColumn("c_decimal", df.col("c_decimal").cast("float"));
            df.show(false);
            return df.where("c_bytea == 'aBC' " +
                            "AND c_float == '1.23'" +
                            "AND c_decimal == '1234566.3446'" +
                            "AND c_numeric == '345.452'" +
                            // interval as milisecond
                            "AND c_interval == '86400000000'" +
                            "").
                    count() == 1;
        });

        // check null values
        await(DATA_TYPES_TABLE, df ->
                df.where("c_text is null AND c_varchar is null AND c_int is null " +
                        "AND c_date is null AND c_timestamp is null AND c_timestamptz is null " +
                        "AND c_float is null AND c_decimal is null AND c_numeric is null AND c_interval is null " +
                        "AND c_boolean is null AND c_uuid is null AND c_bytea is null").count() == 1);
    }

    @Test
    public void testUpdateDeleteDrop() throws Exception {
        //wait for the table to be filled with the existing postgres sample data
        awaitCustomers(df -> df.count() >= 2);

        //there should be a new record with the updated first_name
        runOnPostgres("UPDATE inventory.customers SET first_name='George__UPDATE1' WHERE ID = 1002");
        awaitCustomers(df -> df.where("first_name == 'George__UPDATE1'").count() == 1);

        // add new columns and update the first name again. iceberg should generate the new columns at destination
        runOnPostgres("ALTER TABLE inventory.customers ADD test_varchar_column varchar(255)");
        runOnPostgres("ALTER TABLE inventory.customers ADD test_boolean_column boolean");
        runOnPostgres("ALTER TABLE inventory.customers ADD test_date_column date");
        runOnPostgres("UPDATE inventory.customers SET first_name='George__UPDATE1'  WHERE id = 1002");
        awaitCustomers(df -> df.where("first_name == 'George__UPDATE1'").count() == 2
                && df.where("test_varchar_column is null").count() > 1
                && df.where("test_boolean_column is null").count() > 1
                && df.where("test_date_column is null").count() > 1
        );

        // delete the record
        runOnPostgres("ALTER TABLE inventory.customers ALTER COLUMN email DROP NOT NULL");
        runOnPostgres("INSERT INTO inventory.customers VALUES " +
                "(default,'SallyUSer2','Thomas',null,'value1',false, '2020-01-01')");
        runOnPostgres("ALTER TABLE inventory.customers ALTER COLUMN last_name DROP NOT NULL");
        runOnPostgres("UPDATE inventory.customers SET last_name = NULL  WHERE id = 1002");
        runOnPostgres("DELETE FROM inventory.customers WHERE id = 1004");

        awaitCustomers(ds -> ds.where("first_name == 'George__UPDATE1'").count() == 3
                && ds.where("first_name == 'SallyUSer2'").count() == 1
                && ds.where("last_name is null").count() == 1
                && ds.where("id == '1004'").where("__op == 'd'").count() == 1);


        // drop a column
        runOnPostgres("ALTER TABLE inventory.customers DROP COLUMN email;");
        runOnPostgres("INSERT INTO inventory.customers VALUES " +
                "(default,'User3','lastname_value3','test_varchar_value3',true, '2020-01-01'::DATE)");

        awaitCustomers(ds -> ds.where("first_name == 'User3'").count() == 1
                && ds.where("test_varchar_column == 'test_varchar_value3'").count() == 1);
    }

    private void awaitCustomers(Predicate<Dataset<Row>> predicate) {
        await(CUSTOMERS_TABLE, predicate);
    }

}
