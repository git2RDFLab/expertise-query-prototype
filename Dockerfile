FROM amazoncorretto:21.0.2-alpine3.19
COPY target/*.jar analysis-app.jar

EXPOSE 8082

# Set JVM options to respect container limits and use fewer threads
ENTRYPOINT ["java", \
    "-XX:+UseContainerSupport", \
    "-XX:MaxRAMPercentage=75.0", \
    "-XX:+UseG1GC", \
    "-XX:MaxGCPauseMillis=200", \
    "-XX:ParallelGCThreads=4", \
    "-XX:ConcGCThreads=2", \
    "-Djava.util.concurrent.ForkJoinPool.common.parallelism=4", \
    "-jar", "/analysis-app.jar"]