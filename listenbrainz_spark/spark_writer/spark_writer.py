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

import listenbrainz_spark
from datetime import datetime
from listenbrainz_spark import config, hdfs_connection
from listenbrainz_spark.utils import init_rabbitmq, create_app
from listenbrainz_spark.schema import convert_to_spark_json, listen_schema
from flask import current_app


class SparkWriter:

    def write_message_to_hdfs(self, listens):
        current_app.logger.error(json.dumps(listens, indent=4))
        rows = {}
        for listen in listens:
            dt = datetime.utcfromtimestamp(listen['listened_at'])
            year = dt.year
            month = dt.month
            listen = convert_to_spark_json(listen)
            if year not in rows:
                rows[year] = {}
            if month not in rows[year]:
                rows[year][month] = [listen]
            else:
                rows[year][month].append(listen)
        for year in rows:
            for month in rows[year]:
                df = listenbrainz_spark.session.createDataFrame(rows[year][month], schema=listen_schema)
                df.write.mode('append').parquet(current_app.config['HDFS_CLUSTER_URI'] + '/data/listenbrainz/%s/%s.parquet' % (str(year), str(month)))



    def callback(self, channel, method, properties, body):
        listens = json.loads(body.decode('utf-8'))
        self.write_message_to_hdfs(listens)
        channel.basic_ack(delivery_tag=method.delivery_tag)

    def run(self):
        with create_app().app_context():
            while True:
                hdfs_connection.init_hdfs(current_app.config['HDFS_HTTP_URI'])
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

def main(app_name):
    listenbrainz_spark.init_spark_session(app_name)
    SparkWriter().run()


if __name__ == '__main__':
    main('spark-writer')
