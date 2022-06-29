package io.debezium.server.iomete.debezium;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class OffsetStateTest {

    ByteBuffer key = ByteBuffer.wrap("server1".getBytes(StandardCharsets.UTF_8));
    ByteBuffer val = ByteBuffer.wrap("pos1".getBytes(StandardCharsets.UTF_8));

    Map<ByteBuffer, ByteBuffer> offsets = Map.of(key, val);

    @Test
    public void loadEmptyOffsets() {
        OffsetState offsetState = new OffsetState();

        Map<ByteBuffer, ByteBuffer> expectedOffsets = new HashMap<>();
        expectedOffsets.put(key, null);
        assertThat(offsetState.getOffsets(offsets.keySet()))
                .isEqualTo(expectedOffsets);
    }

    @Test
    public void testSetAndGet() {
        OffsetState offsetState = new OffsetState();

        offsetState.setOffsets(offsets);

        assertThat(offsetState.getOffsets(offsets.keySet()))
                .isEqualTo(offsets);
    }

    @Test
    public void testSaveAndLoad() {
        OffsetState offsetState = new OffsetState();

        offsetState.setOffsets(offsets);

        var state = offsetState.getState();
        OffsetState newOffsetState = new OffsetState();
        newOffsetState.loadState(state);

        assertThat(offsetState.getOffsets(offsets.keySet()))
                .isEqualTo(offsets);
        assertThat(newOffsetState.getOffsets(offsets.keySet()))
                .isEqualTo(offsets);
    }

}