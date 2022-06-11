package io.debezium.server.iomete.performance;

import io.debezium.server.iomete.shared.RandomData;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.debezium.server.iomete.shared.SourcePostgresqlDB.runOnPostgres;

public class Util {
    public static final String TEST_DATE_TABLE_NAME = "default.inventory_test_date_table";

    public static void createTestDataTable() throws Exception {
        // create test table
        String sql = "" +
                "        CREATE TABLE IF NOT EXISTS inventory.test_date_table (\n" +
                "            c_id INTEGER ,\n" +
                "            c_text TEXT,\n" +
                "            c_varchar VARCHAR" +
                "          );";
        runOnPostgres(sql);
    }

    private static final int ROWS_PER_INSERT = 200;

    public static void loadTestDataTable(int numRows, boolean addRandomDelay) {
        final var numberOfThreads = (int) Math.ceil(numRows * 1.0 / ROWS_PER_INSERT);

        for (int t = 0; t < numberOfThreads; t++) {
            new Thread(()-> {
                try {
                    if (addRandomDelay) {
                        Thread.sleep(RandomData.randomInt(20000, 100000));
                    }
                    insertRows();
                } catch (Exception e) {
                    e.printStackTrace();
                    Thread.currentThread().interrupt();
                }
            }).start();

        }
    }

    private static void insertRows() throws Exception {
        var rows = IntStream.range(0, ROWS_PER_INSERT)
                .mapToObj(i -> randomRowGenerator()).collect(Collectors.toList());

        String sql = "INSERT INTO inventory.test_date_table (c_id, c_text, c_varchar ) VALUES "
                + String.join("\n,", rows);

        runOnPostgres(sql);
        runOnPostgres("COMMIT");
    }

    private static String randomRowGenerator() {
        return String.format("(%s, '%s', '%s')",
                RandomData.randomInt(15, 32),
                RandomData.randomString(524),
                RandomData.randomString(524));
    }
}
