FROM openjdk:11-jre-slim
EXPOSE 8080

COPY target/lib /lib/
COPY target/vtl-lab*.jar.original /lib/vtl-lab.jar

ENTRYPOINT ["java", "-cp", "/lib/*", "fr.insee.vtl.lab.VtlLabApplication"]