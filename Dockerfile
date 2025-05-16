FROM iomete.azurecr.io/iomete/spark:3.5.3-v12


COPY ./debezium-server-iomete-sinks/target/debezium-server-iomete-sinks-0.3.0-SNAPSHOT.jar /opt/spark/jars/debezium-server-iomete-sinks-0.3.0-SNAPSHOT.jar