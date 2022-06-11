package io.debezium.server.iomete.batch;

import io.debezium.DebeziumException;

import javax.enterprise.inject.Instance;
import javax.enterprise.inject.literal.NamedLiteral;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchSizeWaitUtil {
  protected static final Logger LOGGER = LoggerFactory.getLogger(BatchSizeWaitUtil.class);

  public static <T> T selectInstance(Instance<T> instances, String name) {

    Instance<T> instance = instances.select(NamedLiteral.of(name));
    if (instance.isAmbiguous()) {
      throw new DebeziumException("Multiple batch size wait class named '" + name + "' were found");
    } else if (instance.isUnsatisfied()) {
      throw new DebeziumException("No batch size wait class named '" + name + "' is available");
    }

    LOGGER.info("Using {}", instance.getClass().getName());
    return instance.get();
  }

}
