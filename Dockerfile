FROM openjdk:11-jre-slim
EXPOSE 8080
ARG JAR_FILE=target/vtl-lab-0.0.1-SNAPSHOT.jar
ADD ${JAR_FILE} app.jar
ENTRYPOINT ["java","-jar","/app.jar"]