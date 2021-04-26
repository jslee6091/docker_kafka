FROM python:3.7.9-stretch

WORKDIR /mykafka

RUN pip install pymysql
RUN pip install kafka-python

COPY ./kafka_consumer.py /mykafka/app.py

CMD ["python", "/mykafka/app.py"]

