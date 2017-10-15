FROM openjdk:8

ENTRYPOINT ["/usr/bin/java", "-jar", "/usr/share/temperature-threshold/temperature-threshold.jar"]

# Add Maven dependencies
ADD target/lib /usr/share/temperature-threshold/lib
ARG JAR_FILE
ADD target/${JAR_FILE} /usr/share/temperature-threshold/temperature-threshold.jar
