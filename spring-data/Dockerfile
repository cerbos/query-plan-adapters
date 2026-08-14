FROM gradle:8.12-jdk17@sha256:deb3ed64f189e9b326e4147de66335286eea9f8b6677e53514af62370cbf7a4a AS build
WORKDIR /app
COPY build.gradle.kts settings.gradle.kts ./
COPY src ./src
# Tests need a Docker daemon (Testcontainers spawns a Cerbos PDP for the adversarial suite) and
# the shared ../conformance/ corpus, neither of which exists inside `docker build`. Build
# artifacts only; run the test suite via CI or `docker run` with the repository root mounted and
# the docker socket passed through (see CLAUDE.md).
RUN gradle build -x test --no-daemon
