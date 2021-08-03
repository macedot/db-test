import os
import time
import logging
import vertica_python
from locust import Locust, TaskSet, task, between, events


def get_sample_query():
    query = '''
        SELECT COUNT(*) FROM tst.test_table
    '''
    conn = {
        'host': os.environ['vertica_host'],
        'port': os.environ['vertica_port'],
        'database': os.environ['vertica_database'],
        'user': os.environ['vertica_user'],
        'password': os.environ['vertica_password'],
        'read_timeout': 600,
        'unicode_error': 'strict',
        'ssl': False
    }
    return conn, query


def execute_query(conn_info, query):
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        cur.execute(query)
        return [x for x in cur.iterate()]


class VerticaTaskSet(TaskSet):
    @task
    def execute_query(self):
        conn_info, query = get_sample_query()
        self.client.execute_query(conn_info, query)


class VerticaClient:
    def __getattr__(self, name):
        def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                res = execute_query(*args, **kwargs)
                events.request_success.fire(request_type="vertica",
                                            name=name,
                                            response_time=int((time.time() - start_time) * 1000),
                                            response_length=len(res))
            except Exception as e:
                events.request_failure.fire(request_type="vertica",
                                            name=name,
                                            response_time=int((time.time() - start_time) * 1000),
                                            exception=e)
                logging.info('error {}'.format(e))
        return wrapper


class VerticaLocust(Locust):
    task_set = VerticaTaskSet
    wait_time = between(0.1, 1)

    def __init__(self):
        super(VerticaLocust, self).__init__()
        self.client = VerticaClient()
