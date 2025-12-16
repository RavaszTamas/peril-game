FROM rabbitmq:3.13-management
EXPOSE 5672
EXPOSE 8080
EXPOSE 15672
RUN rabbitmq-plugins enable rabbitmq_stomp