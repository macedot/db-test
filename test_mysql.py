import sys
import time
import random
import logging

from uuid import uuid4
from locust import User, task
from sqlalchemy import create_engine


def create_conn(conn_string):
    return create_engine('mysql+pymysql://' + conn_string).connect()


def execute_query(conn, query):
    rs = conn.execute(query)
    return rs


class MySqlClient:

    def __init__(self, request_event):
        super().__init__()
        self._request_event = request_event

    def __getattr__(self, name):
        def wrapper(*args, **kwargs):
            start_time = time.perf_counter()
            request_meta = {
                "request_type": "mysql",
                "name": name,
                "response_length": 0,
                "response": None,
                "context": {},
                "exception": None,
            }

            try:
                res = execute_query(*args, **kwargs)
                request_meta["response"] = res
                request_meta['response_length'] = res.rowcount
            except Exception as e:
                logging.info('error {}'.format(e))
                request_meta['exception'] = e

            request_meta['response_time'] = int((time.perf_counter() - start_time) * 1000)
            self._request_event.fire(**request_meta)  # This is what makes the request actually get logged in Locust
        return wrapper


class MySqlUser(User):
    abstract = True  # dont instantiate this as an actual user when running Locust

    def __init__(self, environment):
        super()
        self.client = MySqlClient(request_event=environment.events.request)


class TestMySql(MySqlUser):
    conn_string = 'root@localhost:3306/test'
    conn = None

    def __init__(self, environment):
        super().__init__(environment)
        self.conn = create_conn(self.conn_string)
        # create_table = '''
        # CREATE TABLE `locust` (
        #     `AUTOSEQUENCIAL` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT,
        #     `NAME` TINYTEXT NOT NULL COLLATE 'latin1_swedish_ci',
        #     `VALUE` BIGINT(20) UNSIGNED NOT NULL DEFAULT '0',
        #     PRIMARY KEY (`AUTOSEQUENCIAL`) USING BTREE
        # )
        # ENGINE=InnoDB
        # ;
        # '''
        # execute_query(self.conn, create_table)

    # @task
    # def query_select_all(self):
    #     self.client.execute_query(conn=self.conn,
    #                               query="SELECT * FROM locust")

    @task
    def query_insert_one(self):
        name = str(uuid4())
        value = random.randint(0, sys.maxsize)
        query = f"INSERT INTO locust (NAME,VALUE) VALUES ('{name}',{value})"
        self.client.execute_query(conn=self.conn, query=query)
