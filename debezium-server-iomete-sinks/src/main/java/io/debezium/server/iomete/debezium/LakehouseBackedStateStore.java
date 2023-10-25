package io.debezium.server.iomete.debezium;

import io.debezium.document.DocumentReader;
import io.debezium.document.DocumentWriter;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.server.iomete.SparkProvider;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.util.SafeObjectInputStream;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.*;

import static java.lang.String.format;

public class LakehouseBackedStateStore {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final StructType SCHEMA =
            StructType.fromDDL("offsets string, database_history string");

    private final String table = "default.cdc_state";

    private final OffsetState offsetState;
    private final DatabaseHistoryState databaseHistoryState;

    private final SparkSession spark;

    public LakehouseBackedStateStore(
            OffsetState offsetState, DatabaseHistoryState databaseHistoryState, SparkSession spark) {
        this.offsetState = offsetState;
        this.databaseHistoryState = databaseHistoryState;
        this.spark = spark;
    }

    public void start() {
        loadSnapshot();
    }

    public void storeHistoryRecord(HistoryRecord record) {
        databaseHistoryState.storeHistoryRecord(record);
    }

    public List<HistoryRecord> historyRecords() {
        return databaseHistoryState.historyRecords();
    }

    public void setOffsets(final Map<ByteBuffer, ByteBuffer> offsets) {
        offsetState.setOffsets(offsets);
    }

    public Map<ByteBuffer, ByteBuffer> getOffsets(final Collection<ByteBuffer> keys) {
        return offsetState.getOffsets(keys);
    }

    public void loadSnapshot() {
        Optional<Tuple2<String, String>> stateMaybe = loadStateFromSpark();
        if (stateMaybe.isPresent()) {
            var state = stateMaybe.get();

            offsetState.loadState(state._1);
            databaseHistoryState.loadState(state._2);
        }
    }

    public void takeSnapshot() throws IOException {
        String offsets =  offsetState.getState();
        String databaseHistory = databaseHistoryState.getState();

        var rows = List.of(RowFactory.create(offsets, databaseHistory));
        var df = spark.createDataFrame(rows, SCHEMA);

        df.writeTo(table).replace();
    }

    private Optional<Tuple2<String, String>> loadStateFromSpark() {
        try {
            var df = spark.table(table);
            var rows = df.collectAsList();
            if (rows.size() == 0) {
                return Optional.empty();
            }

            var firstRow = rows.get(0);
            String offsets = firstRow.getString(0);
            String databaseHistory = firstRow.getString(1);


            return Optional.of(new Tuple2<>(offsets, databaseHistory));

        } catch (Exception analysisException) {
            if (!analysisException.getMessage().startsWith("Table or view not found")) {
                logger.error("AnalysisException on load table", analysisException);
                throw new RuntimeException("Error while loading table");
            }
            //table doesn't exist, create it
            spark.sql(format("create table %s(%s)", table, SCHEMA.toDDL()));
        }

        return Optional.empty();
    }
}

class LakehouseBackedStateStoreProvider {
    private static LakehouseBackedStateStore instance;

    public static LakehouseBackedStateStore instance() {
        if (instance == null) {
            instance = new LakehouseBackedStateStore(
                    new OffsetState(), new DatabaseHistoryState(), SparkProvider.createSparkSession());
        }
        return instance;
    }
}

class DatabaseHistoryState {
    private final List<HistoryRecord> historyRecords = new ArrayList<>();

    private final DocumentWriter writer = DocumentWriter.defaultWriter();
    private final DocumentReader reader = DocumentReader.defaultReader();

    public void storeHistoryRecord(HistoryRecord record) {
        if (record == null) {
            return;
        }

        historyRecords.add(record);
    }

    public List<HistoryRecord> historyRecords() {
        return historyRecords;
    }

    public void loadState(String databaseHistory) {
        try (Scanner scanner = new Scanner(databaseHistory)) {
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                if (line != null && !line.isEmpty()) {

                    historyRecords.add(new HistoryRecord(reader.read(line)));

                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String getState() throws IOException {
        StringBuilder stringBuilder = new StringBuilder();
        for (HistoryRecord historyRecord : historyRecords) {
            String line = writer.write(historyRecord.document());
            stringBuilder.append(line);
            stringBuilder.append("\n");
        }
        return stringBuilder.toString();
    }
}

class OffsetState {
    private final Map<ByteBuffer, ByteBuffer> offsetData = new HashMap<>();

    public void setOffsets(final Map<ByteBuffer, ByteBuffer> offsets) {
        offsetData.putAll(offsets);
    }

    public Map<ByteBuffer, ByteBuffer> getOffsets(final Collection<ByteBuffer> keys) {
        Map<ByteBuffer, ByteBuffer> result = new HashMap<>();
        for (ByteBuffer key : keys) {
            result.put(key, offsetData.get(key));
        }
        return result;
    }

    public void loadState(String offsetBase64) {
        byte[] decodedString = Base64.getDecoder().decode(offsetBase64);
        try (SafeObjectInputStream is = new SafeObjectInputStream(new ByteArrayInputStream(decodedString))) {
            Object obj = is.readObject();
            if (!(obj instanceof HashMap))
                throw new ConnectException("Expected HashMap but found " + obj.getClass());
            Map<byte[], byte[]> raw = (Map<byte[], byte[]>) obj;
            for (Map.Entry<byte[], byte[]> mapEntry : raw.entrySet()) {
                ByteBuffer key = (mapEntry.getKey() != null) ? ByteBuffer.wrap(mapEntry.getKey()) : null;
                ByteBuffer value = (mapEntry.getValue() != null) ? ByteBuffer.wrap(mapEntry.getValue()) : null;
                offsetData.put(key, value);
            }
        } catch (IOException | ClassNotFoundException e) {
            throw new ConnectException(e);
        }
    }

    public String getState() {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        //take snapshot of offset store
        try (ObjectOutputStream os = new ObjectOutputStream(byteArrayOutputStream)) {
            Map<byte[], byte[]> raw = new HashMap<>();
            for (Map.Entry<ByteBuffer, ByteBuffer> mapEntry : offsetData.entrySet()) {
                byte[] key = (mapEntry.getKey() != null) ? mapEntry.getKey().array() : null;
                byte[] value = (mapEntry.getValue() != null) ? mapEntry.getValue().array() : null;
                raw.put(key, value);
            }
            os.writeObject(raw);
        } catch (IOException ignored) {
        }

        return Base64.getEncoder().encodeToString(byteArrayOutputStream.toByteArray());
    }

}
