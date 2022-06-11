package io.debezium.server.iomete;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.spark.sql.types.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DebeziumSparkEvent {
    protected static final Logger LOGGER = LoggerFactory.getLogger(DebeziumSparkEvent.class);

    protected final String destination;

    protected final JsonNode value;
    protected final JsonNode key;
    protected final JsonNode valueSchema;
    protected final JsonNode keySchema;

    public DebeziumSparkEvent(String destination, JsonNode value, JsonNode key, JsonNode valueSchema, JsonNode keySchema) {
        this.destination = destination;
        this.value = value;
        this.key = key;
        this.valueSchema = valueSchema;
        this.keySchema = keySchema;
    }

    public String destination() {
        return destination;
    }

    public JsonNode value() {
        return value;
    }

    public JsonNode key() {
        return key;
    }

    public JsonNode valueSchema() {
        return valueSchema;
    }

    public JsonNode keySchema() {
        return keySchema;
    }

    public List<String> keyColumnNames() {
        boolean hasKeyFields = keySchema != null && keySchema.has("fields") && keySchema.get("fields").isArray();
        if (!hasKeyFields) {
            return List.of();
        }

        List<String> fieldNames = new ArrayList<>();
        for (JsonNode jsonSchemaFieldNode : keySchema.get("fields")) {
            String fieldName = jsonSchemaFieldNode.get("field").textValue();
            fieldNames.add(fieldName);
        }
        return fieldNames;
    }

    private static StructType getSparkDfSchema(JsonNode schemaNode) {

        if (schemaNode == null) {
            return null;
        }

        StructType sparkSchema = new StructType();

        String schemaType = schemaNode.get("type").textValue();
        String schemaName = "root";
        if (schemaNode.has("field")) {
            schemaName = schemaNode.get("field").textValue();
        }
        LOGGER.trace("Converting Schema of: {}::{}", schemaName, schemaType);

        for (JsonNode jsonSchemaFieldNode : schemaNode.get("fields")) {
            String fieldName = jsonSchemaFieldNode.get("field").textValue();
            String fieldType = jsonSchemaFieldNode.get("type").textValue();
            LOGGER.trace("Processing Field: {}.{}::{}", schemaName, fieldName, fieldType);
            // for all the debezium data types please see org.apache.kafka.connect.data.Schema;
            switch (fieldType) {
                case "int8":
                case "int16":
                case "int32":
                    sparkSchema = sparkSchema.add(new StructField(fieldName, DataTypes.IntegerType, true, Metadata.empty()));
                    break;
                case "int64":
                    sparkSchema = sparkSchema.add(new StructField(fieldName, DataTypes.LongType, true, Metadata.empty()));
                    break;
                case "float8":
                case "float16":
                case "float32":
                    sparkSchema = sparkSchema.add(new StructField(fieldName, DataTypes.FloatType, true, Metadata.empty()));
                    break;
                case "float64":
                    sparkSchema = sparkSchema.add(new StructField(fieldName, DataTypes.DoubleType, true, Metadata.empty()));
                    break;
                case "boolean":
                    sparkSchema = sparkSchema.add(new StructField(fieldName, DataTypes.BooleanType, true, Metadata.empty()));
                    break;
                case "bytes":
                    sparkSchema = sparkSchema.add(new StructField(fieldName, DataTypes.BinaryType, true, Metadata.empty()));
                    break;
                case "array":
                    sparkSchema = sparkSchema.add(new StructField(fieldName, new ArrayType(), true, Metadata.empty()));
                    break;
                case "map":
                    sparkSchema = sparkSchema.add(new StructField(fieldName, new MapType(), true, Metadata.empty()));
                    break;
                case "struct":
                    // recursive call
                    StructType subSchema = getSparkDfSchema(jsonSchemaFieldNode);
                    sparkSchema = sparkSchema.add(new StructField(fieldName, subSchema, true, Metadata.empty()));
                    break;
                default:
                    // default to String type
                    sparkSchema = sparkSchema.add(new StructField(fieldName, DataTypes.StringType, true, Metadata.empty()));
                    break;
            }
        }

        return sparkSchema;

    }

    public StructType getSparkDfSchema() {
        StructType dfSchema = getSparkDfSchema(this.valueSchema());
        if (dfSchema == null) {
            return null;
        }

        // partitioning ts field added by the consumer
        dfSchema = dfSchema.add(new StructField("__source_ts", DataTypes.TimestampType, true, Metadata.empty()));
        dfSchema = dfSchema.add(new StructField("__op_num", DataTypes.IntegerType, true, Metadata.empty()));

        // special destinations like "heartbeat.topics" might not have __source_ts_ms field.
        if (!Arrays.asList(dfSchema.fieldNames()).contains("__source_ts_ms")) {
            dfSchema = dfSchema.add(new StructField("__source_ts_ms", DataTypes.LongType, true, Metadata.empty()));
        }
        return dfSchema;
    }

}