import errno
import logging
import os
import pika

from brainzutils.flask import CustomFlask
from time import sleep


APP_CREATION_RETRY_COUNT = 10


def create_path(path):
    try:
        os.makedirs(path)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise


def create_app():
    app = CustomFlask(import_name=__name__)
    config_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'config.py')
    for i in range(APP_CREATION_RETRY_COUNT):
        if not os.path.exists(config_file):
            sleep(1)
    app.config.from_pyfile(config_file)
    app.init_loggers(
        file_config=app.config.get('LOG_FILE'),
        email_config=app.config.get('LOG_EMAIL'),
        sentry_config=app.config.get('LOG_SENTRY'),
    )
    return app


def init_rabbitmq(username, password, host, port, vhost, log=logging.error):
    while True:
        try:
            credentials = pika.PlainCredentials(username, password)
            connection_parameters = pika.ConnectionParameters(
                host=host,
                port=port,
                virtual_host=vhost,
                credentials=credentials,
            )
            return pika.BlockingConnection(connection_parameters)
        except Exception as e:
            log('Error while connecting to RabbitMQ', exc_info=True)
            sleep(1)
