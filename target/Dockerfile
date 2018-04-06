FROM java:8
WORKDIR /
ADD Producer-0.0.1-SNAPSHOT-jar-with-dependencies.jar Producer-0.0.1-SNAPSHOT-jar-with-dependencies.jar
EXPOSE 8080
CMD java - cp Producer-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.ABC.Producer jsonTest, 2, 10.17.01.01:2181

