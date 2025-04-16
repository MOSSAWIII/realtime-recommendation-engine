FROM sbtscala/scala-sbt:eclipse-temurin-jammy-17.0.8.1_1_1.9.6_2.13.12

WORKDIR /app

# Copier d'abord les fichiers de configuration de build pour tirer parti du cache Docker
COPY project/ project/
COPY build.sbt .

# Pré-télécharger les dépendances SBT avant de copier le code source
RUN sbt update

# Ensuite copier le code source
COPY src/ src/
COPY src/main/resources/ resources/

RUN sbt compile

EXPOSE 8080

CMD ["sbt", "run"]