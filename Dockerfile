FROM openjdk:8-jre

WORKDIR /usr/share/taskflow

RUN mkdir -p bin
RUN mkdir -p lib

COPY src/main/bin/taskflow.sh ./bin/
COPY target/taskflow.jar ./lib/

RUN chmod +x ./bin/taskflow.sh

ENV PATH /usr/share/taskflow/bin:$PATH

EXPOSE 8080

CMD ["bin/taskflow.sh"]