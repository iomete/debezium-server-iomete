package io.debezium.server.iomete.debezium;


import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class LakehouseBackedStateStoreProviderTest {

    @Test
    public void multipleCallsReturnsSameInstance(){
        assertThat(LakehouseBackedStateStoreProvider.instance())
                .isEqualTo(LakehouseBackedStateStoreProvider.instance());
    }

}