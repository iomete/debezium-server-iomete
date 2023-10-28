package io.debezium.server.iomete;

import io.debezium.serde.DebeziumSerdes;
import io.debezium.util.Testing;

import java.util.Collections;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serde;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class DebeziumSparkEventTest {

//  final String serdeWithSchema = Testing.Files.readResourceAsString("json/serde-with-schema.json");
//  final String unwrapWithSchema = Testing.Files.readResourceAsString("json/unwrap-with-schema.json");
//
//  public StructType getEventSparkDfSchema(String event) throws JsonProcessingException {
//    JsonNode jsonNode = new ObjectMapper().readTree(event);
//
//    DebeziumSparkEvent e = new DebeziumSparkEvent("test",
//        jsonNode.get("payload"),
//        null,
//        jsonNode.get("schema"),
//        null
//    );
//    return e.getSparkDfSchema();
//  }
//
//  @Test
//  public void testSimpleSchema() throws JsonProcessingException {
//    StructType schema = getEventSparkDfSchema(unwrapWithSchema);
//
//    assertThat(schema).isNotNull();
//    assertThat(schema.catalogString())
//            .contains("id:int,order_date:int,purchaser:int,quantity:int,product_id:int,__op:string");
//  }
//
//  @Test
//  public void testNestedSparkSchema() throws JsonProcessingException {
//    StructType schema = getEventSparkDfSchema(serdeWithSchema);
//    assertThat(schema).isNotNull();
//
//    assertThat(schema.catalogString())
//            .contains("before:struct<id");
//    assertThat(schema.catalogString())
//            .contains("after:struct<id");
//  }
//
//  @Test
//  public void testValuePayloadWithSchemaAsJsonNode() {
//    // testing Debezium deserializer
//    final Serde<JsonNode> valueSerde = DebeziumSerdes.payloadJson(JsonNode.class);
//    valueSerde.configure(Collections.emptyMap(), false);
//
//    JsonNode deserializedData = valueSerde.deserializer()
//            .deserialize("xx", serdeWithSchema.getBytes());
//
//    assertThat(deserializedData.getClass().getSimpleName())
//            .isEqualTo("ObjectNode");
//
//    assertThat(deserializedData.has("after")).isTrue();
//    assertThat(deserializedData.has("op")).isTrue();
//    assertThat(deserializedData.has("before")).isTrue();
//    assertThat(deserializedData.has("schema")).isFalse();
//
//    valueSerde.configure(Collections.singletonMap("from.field", "schema"), false);
//    JsonNode deserializedSchema = valueSerde.deserializer().deserialize("xx", serdeWithSchema.getBytes());
//    assertFalse(deserializedSchema.has("schema"));
//  }

}
