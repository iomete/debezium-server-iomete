package io.debezium.server.iomete;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import io.debezium.DebeziumException;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import io.debezium.serde.DebeziumSerdes;
import io.debezium.server.BaseChangeConsumer;
import io.debezium.server.iomete.batch.BatchSizeWaitUtil;
import io.debezium.server.iomete.batch.InterfaceBatchSizeWait;
import io.debezium.util.Clock;
import io.debezium.util.Strings;
import io.debezium.util.Threads;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public abstract class AbstractChangeConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {
  protected final Logger LOGGER = LoggerFactory.getLogger(getClass());

  protected static final Duration LOG_INTERVAL = Duration.ofMinutes(15);

  protected static final ConcurrentHashMap<String, Object> uploadLock = new ConcurrentHashMap<>();

  protected static final ObjectMapper mapper = new ObjectMapper();

  protected static final Serde<JsonNode> valSerde = DebeziumSerdes.payloadJson(JsonNode.class);
  protected static final Serde<JsonNode> keySerde = DebeziumSerdes.payloadJson(JsonNode.class);
  static Deserializer<JsonNode> keyDeserializer;
  protected Deserializer<JsonNode> valDeserializer;

  protected final Clock clock = Clock.system();

  protected long consumerStart = clock.currentTimeInMillis();
  protected long numConsumedEvents = 0;
  protected Threads.Timer logTimer = Threads.timer(clock, LOG_INTERVAL);
  @ConfigProperty(name = "debezium.format.value", defaultValue = "json")
  String valueFormat;
  @ConfigProperty(name = "debezium.format.key", defaultValue = "json")
  String keyFormat;
  @ConfigProperty(name = "debezium.sink.batch.batch-size-wait", defaultValue = "NoBatchSizeWait")
  String batchSizeWaitName;

  public static final ImmutableMap<String, Integer> CDC_OPERATIONS = ImmutableMap.of(
          "c", 1, "r", 2, "u", 3, "d", 4);

  @Inject
  @Any
  Instance<InterfaceBatchSizeWait> batchSizeWaitInstances;
  InterfaceBatchSizeWait batchSizeWait;

  public void initizalize() throws InterruptedException {
    LOGGER.info("initizalize");
    // configure and set 
    valSerde.configure(Collections.emptyMap(), false);
    valDeserializer = valSerde.deserializer();
    // configure and set 
    keySerde.configure(Collections.emptyMap(), true);
    keyDeserializer = keySerde.deserializer();

    if (!valueFormat.equalsIgnoreCase(Json.class.getSimpleName().toLowerCase())) {
      throw new InterruptedException("debezium.format.value={" + valueFormat + "} not supported! Supported (debezium.format.value=*) formats are {json,}!");
    }

    if (!keyFormat.equalsIgnoreCase(Json.class.getSimpleName().toLowerCase())) {
      throw new InterruptedException("debezium.format.key={" + valueFormat + "} not supported! Supported (debezium.format.key=*) formats are {json,}!");
    }

    batchSizeWait = BatchSizeWaitUtil.selectInstance(batchSizeWaitInstances, batchSizeWaitName);
    LOGGER.info("Using {} to optimize batch size", batchSizeWait.getClass().getSimpleName());
    batchSizeWait.initizalize();
  }

  @Override
  public void handleBatch(List<ChangeEvent<Object, Object>> records, DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer)
      throws InterruptedException {
    LOGGER.trace("Received {} events", records.size());

    Instant start = Instant.now();
    Map<String, List<DebeziumSparkEvent>> events = records.stream()
        .map((ChangeEvent<Object, Object> e)
            -> {
          try {
            return new DebeziumSparkEvent(e.destination(),
                getPayload(e.destination(), e.value()), //valDeserializer.deserialize(e.destination(), getBytes(e.value())),
                e.key() == null ? null : keyDeserializer.deserialize(e.destination(), getBytes(e.key())),
                mapper.readTree(getBytes(e.value())).get("schema"),
                e.key() == null ? null : mapper.readTree(getBytes(e.key())).get("schema")
            );
          } catch (IOException ex) {
            throw new DebeziumException(ex);
          }
        })
        .collect(Collectors.groupingBy(DebeziumSparkEvent::destination));

    long numUploadedEvents = 0;
    for (Map.Entry<String, List<DebeziumSparkEvent>> destinationEvents : events.entrySet()) {
      // group list of events by their schema, if in the batch we have schema change events grouped by their schema
      // so with this uniform schema is guaranteed for each batch
      Map<JsonNode, List<DebeziumSparkEvent>> eventsGroupedBySchema =
          destinationEvents.getValue().stream()
              .collect(Collectors.groupingBy(DebeziumSparkEvent::valueSchema));
      LOGGER.debug("Batch got {} records with {} different schema!!", destinationEvents.getValue().size(),
          eventsGroupedBySchema.keySet().size());

      for (List<DebeziumSparkEvent> schemaEvents : eventsGroupedBySchema.values()) {
        numUploadedEvents += this.uploadDestination(destinationEvents.getKey(), schemaEvents);
      }
    }

    for (ChangeEvent<Object, Object> record : records) {
      LOGGER.trace("Processed event '{}'", record);
      committer.markProcessed(record);
    }
    committer.markBatchFinished();

    this.logConsumerProgress(numUploadedEvents);

    LOGGER.debug("Received:{} Processed:{} events", records.size(), numUploadedEvents);

    batchSizeWait.waitMs(numUploadedEvents, (int) Duration.between(start, Instant.now()).toMillis());
  }

  protected void logConsumerProgress(long numUploadedEvents) {
    numConsumedEvents += numUploadedEvents;
    if (logTimer.expired()) {
      LOGGER.info("Consumed {} records after {}", numConsumedEvents, Strings.duration(clock.currentTimeInMillis() - consumerStart));
      numConsumedEvents = 0;
      consumerStart = clock.currentTimeInMillis();
      logTimer = Threads.timer(clock, LOG_INTERVAL);
    }
  }

  public JsonNode getPayload(String destination, Object val) {
    JsonNode pl = valDeserializer.deserialize(destination, getBytes(val));
    // used to partition tables __source_ts
    if (pl.has("__source_ts_ms")) {
      ((ObjectNode) pl).put("__source_ts", pl.get("__source_ts_ms").longValue() / 1000);
    } else {
      ((ObjectNode) pl).put("__source_ts", Instant.now().getEpochSecond());
      ((ObjectNode) pl).put("__source_ts_ms", Instant.now().toEpochMilli());
    }

    ((ObjectNode) pl).put("__op_num", CDC_OPERATIONS.get(pl.get("__op").asText()));

    return pl;
  }

  public abstract long uploadDestination(String destination, List<DebeziumSparkEvent> data) throws InterruptedException;

}
