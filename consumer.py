from kafka import KafkaConsumer

import psycopg2

class DataWriter():
    def __init__(self, db, user, password, host, port, table):
        self.conn = psycopg2.connect(dbname=db, user=user, password=password, host=host, port=port)
        self.table = table
        self.cur = self.conn.cursor()

    def add_data(self, site, response_time, status_code, regexp_check, time):
        query = (f"INSERT INTO {self.table} "
                 "(site, response_time, status_code, regexp_check, time) "
                 "VALUES (%s, %s, %s, %s);")
        self.cur.execute(query,(site, response_time, status_code, regexp_check, time))

    def close(self):
        self.cur.close()
        self.conn.close()

class Consumer:

    def __init__(self, config):
        self.server = config['kafka']['server']
        self.topic = config['kafka']['topic']

        self.consumer = KafkaConsumer(bootstrap_servers=self.server,
                                 auto_offset_reset='earliest',
                                 consumer_timeout_ms=1000)
        consumer.subscribe(self.topic)

        self.datawriter = DataWriter(config['postgres']['db'],
                                    config['postgres']['user'],
                                    config['postgres']['password'],
                                    config['postgres']['host'],
                                    config['postgres']['port'],
                                    config['postgres']['table'])

    def consume(self):
        while not self.stop_event.is_set():
            for message in self.consumer:
                print(message)
                self.datawriter.add_data(message['site'],
                        message['response_time'],
                        message['status_code'],
                        message['regexp_check'],
                        message['time']
                        )
                if self.stop_event.is_set():
                    break

        consumer.close()