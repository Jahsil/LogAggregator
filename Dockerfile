# ----------------------------
# Dockerfile for Scala App
# ----------------------------

FROM eclipse-temurin:11-jdk-jammy

# Install curl, unzip, gnupg
RUN apt-get update && \
    apt-get install -y curl unzip gnupg && \
    rm -rf /var/lib/apt/lists/*

# Install sbt
RUN echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | tee /etc/apt/sources.list.d/sbt.list && \
    curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x99E82A75642AC823" | apt-key add - && \
    apt-get update && \
    apt-get install -y sbt && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Cache dependencies
COPY build.sbt .
COPY project ./project
RUN sbt update

# Copy source
COPY src ./src

# Build fat jar
RUN sbt assembly
RUN cp target/scala-2.13/*assembly*.jar app.jar

# ENTRYPOINT lets docker-compose override the class
ENTRYPOINT ["java", "-cp", "app.jar"]
CMD ["com.example.logagg.LogGenerator"]
