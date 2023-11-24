FROM openjdk:17-jdk-slim
EXPOSE 8080

COPY target/lib /lib/

COPY target/lib/vtl-spark-*.jar /vtl-spark.jar
COPY target/lib/vtl-model-*.jar /vtl-model.jar
COPY target/lib/vtl-engine-*.jar /vtl-engine.jar
COPY target/lib/vtl-jackson-*.jar /vtl-jackson.jar
COPY target/lib/vtl-parser-*.jar /vtl-parser.jar
COPY target/lib/postgresql-*.jar /postgresql.jar
COPY target/lib/postgis-jdbc-*.jar /postgis-jdbc.jar

COPY target/trevas-lab*.jar.original /lib/trevas-lab.jar

ENTRYPOINT ["java", "-cp", "/lib/*", "fr.insee.trevas.lab.TrevasLabApplication"]