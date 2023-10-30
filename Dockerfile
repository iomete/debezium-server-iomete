FROM iomete/spark:3.3.6-latest


COPY ./debezium-server-iomete-sinks/target/debezium-server-iomete-sinks-0.3.0-SNAPSHOT.jar /opt/spark/jars/debezium-server-iomete-sinks-0.3.0-SNAPSHOT.jar