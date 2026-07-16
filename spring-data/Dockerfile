FROM gradle:8.12-jdk17 AS build
WORKDIR /app
COPY build.gradle.kts settings.gradle.kts ./
COPY src ./src
# Tests need a Docker daemon (Testcontainers spawns a Cerbos PDP) and the shared /policies
# directory, neither of which exists inside `docker build`. Build artifacts only; run the
# test suite via CI or `docker run` with the docker socket mounted (see CLAUDE.md).
RUN gradle build -x test --no-daemon
