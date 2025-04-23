FROM bitnami/spark:3.5.5
USER root
RUN install_packages curl
USER 1001
RUN curl https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.5/postgresql-42.7.5.jar --output $SPARK_HOME/jars/postgresql-42.7.5.jar
RUN $SPARK_HOME/venv/bin/pip install grpcio