FROM sbtscala/scala-sbt:eclipse-temurin-jammy-17.0.8.1_1_1.9.6_2.13.12

WORKDIR /app

# Copier les fichiers de configuration SBT
COPY project/build.properties project/
COPY project/plugins.sbt project/
COPY build.sbt .

# Copier le code source
COPY src/ src/
COPY database/ database/
COPY docs/ docs/

# Compiler l'application
RUN sbt clean compile

# Exposer le port (si nécessaire pour une API REST)
EXPOSE 8080

# Commande par défaut pour exécuter l'application
CMD ["sbt", "run"]