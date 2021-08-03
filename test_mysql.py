import logging
import time

from locust import User, between, TaskSet, task, events
from sqlalchemy import create_engine


def create_conn(conn_string):
    print("Connecting to MySQL")
    return create_engine('mysql+pymysql://' + conn_string).connect()


def execute_query(conn_string, query):
    _conn = create_conn(conn_string)
    rs = _conn.execute(query)
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
    min_wait = 0
    max_wait = 0
    wait_time = between(min_wait, max_wait)

    conn_string = '2412923313019:2412923313019@localhost:3306/DEV_ezCheckB_Sincronica'

    @task
    def execute_query(self):
        self.client.execute_query(conn_string=self.conn_string,
                                  query="SELECT * FROM DUMP_20200603")
