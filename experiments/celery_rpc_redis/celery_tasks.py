from celery import Celery

app = Celery('tasks', backend='redis://localhost', broker='pyamqp://guest@localhost//')
app.conf.task_reject_on_worker_lost = True
app.conf.task_acks_late = True
app.conf.broker_pool_limit = None


@app.task
def test_task():
    return 1
