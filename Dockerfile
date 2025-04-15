FROM sbtscala/scala-sbt:eclipse-temurin-jammy-17.0.8.1_1_1.9.6_2.13.12

WORKDIR /app

COPY project/build.properties project/
COPY project/plugins.sbt project/
COPY build.sbt .

COPY src/ src/
COPY src/main/resources/ resources/

RUN sbt clean compile

EXPOSE 8080

CMD ["sbt", "run"]