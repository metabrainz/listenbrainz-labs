# listenbrainz-recommendation-playground
# Copyright (C) 2018 MetaBrainz Foundation Inc.
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.

import os
import pika
import uuid
import json

from listenbrainz_spark import config, hdfs_connection
from listenbrainz_spark.utils import init_rabbitmq, create_app
from flask import current_app

HDFS_FILE_SIZE_LIMIT = 1024 * 1024 * 1024 # 1 GB


class SparkWriter:

    def write_message_to_hdfs(self, listens):
        current_app.logger.error(json.dumps(listens, indent=4))
        data = self._convert_listens_to_string(listens)
        path = self._create_path(self.current_file)
        hdfs_connection.client.write(path, data=data, append=True)
        if self._get_size(self.current_file) > HDFS_FILE_SIZE_LIMIT:
            self.current_file = self._new_file()

    def callback(self, channel, method, properties, body):
        listens = json.loads(body.decode('utf-8'))
        self.write_message_to_hdfs(listens)
        channel.basic_ack(delivery_tag=method.delivery_tag)

    def run(self):
        with create_app().app_context():
            while True:
                hdfs_connection.init_hdfs(current_app.config['HDFS_NAMENODE_URI'])
                self.current_file = self._new_file()
                rabbitmq = init_rabbitmq(
                    username=current_app.config['RABBITMQ_USERNAME'],
                    password=current_app.config['RABBITMQ_PASSWORD'],
                    host=current_app.config['RABBITMQ_HOST'],
                    port=current_app.config['RABBITMQ_PORT'],
                    vhost=current_app.config['RABBITMQ_VHOST'],
                    log=current_app.logger.critical,
                )
                channel = rabbitmq.channel()
                channel.exchange_declare(exchange=current_app.config['INCOMING_EXCHANGE'], exchange_type='fanout')
                channel.queue_declare(current_app.config['INCOMING_QUEUE'], durable=True)
                channel.queue_bind(exchange=current_app.config['INCOMING_EXCHANGE'], queue=current_app.config['INCOMING_QUEUE'])
                channel.basic_consume(self.callback, queue=current_app.config['INCOMING_QUEUE'])

                current_app.logger.info('Started spark-writer...')
                try:
                    channel.start_consuming()
                except pika.exceptions.ConnectionClosed:
                    current_app.logger.error('connection to rabbitmq closed.', exc_info=True)
                    continue
                rabbitmq.close()

    def _create_path(self, filename):
        return os.path.join('/data', 'listenbrainz', filename[0], filename[0:2], filename)

    def _get_size(self, filename):
        return hdfs_connection.client.status(self._create_path(filename))['length']

    def _convert_listens_to_string(self, listens):
        data = ''
        for listen in listens:
            data += json.dumps(listen) + '\n'
        return data

    def _new_file(self):
        filename = str(uuid.uuid4()) + '.listens'
        hdfs_connection.client.write(self._create_path(filename), data='')
        return filename


def main():
    SparkWriter().run()


if __name__ == '__main__':
    main()
