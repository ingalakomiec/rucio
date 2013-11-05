# Copyright European Organization for Nuclear Research (CERN)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Mario Lassnig, <mario.lassnig@cern.ch>, 2013

"""
Conveyor is a daemon to manage file transfers.
"""

import datetime
import threading
import time

import json
import stomp

from rucio.db.constants import FTSState, RequestState
from rucio.core.monitor import record_counter
from rucio.core.request import set_request_state
from rucio.common.config import config_get, config_get_int

graceful_stop = threading.Event()


class Consumer(object):

    def __init__(self, broker):
        self.__broker = broker

    def on_error(self, headers, message):
        record_counter('daemons.conveyor.consumer.error')
        print '[%s %s] ERROR: %s' % (self.__broker, datetime.datetime.now(), message)

    def on_message(self, headers, message):
        print '[%s %s] MESSAGE: %s' % (self.__broker, datetime.datetime.now(), message['file_state'])
        record_counter('daemons.conveyor.consumer.message')
        msg = json.loads(message[:-1])  # message always ends with an unparseable EOT character

        if msg['job_metadata'] != '':
            if msg['job_state'] == FTSState.FINISHED:
                print 'job finished'
                set_request_state(msg['job_metadata']['request_id'], RequestState.DONE)


def consumer(once=False, process=0, total_processes=1, thread=0, total_threads=1):
    """
    Main loop to consume messages from the FTS3 producer.
    """

    print 'consumer: starting'

    brokers = []
    try:
        brokers = [b.strip() for b in config_get('messaging-fts3', 'brokers').split(',')]
    except:
        raise Exception('Could not load brokers from configuration')

    conns = []
    for broker in brokers:
        conns.append(stomp.Connection(host_and_ports=[(broker, config_get_int('messaging-fts3', 'port'))],
                                      use_ssl=True,
                                      ssl_key_file=config_get('messaging-fts3', 'ssl_key_file'),
                                      ssl_cert_file=config_get('messaging-fts3', 'ssl_cert_file')))

    print 'consumer: started'

    while not graceful_stop.is_set():

        for conn in conns:

            if not conn.is_connected():

                print 'consumer: connecting to', conn._Connection__host_and_ports[0][0]
                record_counter('daemons.messaging.fts3.reconnect.%s' % conn._Connection__host_and_ports[0][0].split('.')[0])

                conn.set_listener('rucio-messaging-fts3', Consumer(broker=conn._Connection__host_and_ports[0]))
                conn.start()
                conn.connect(headers={'client-id': 'rucio-messaging-fts3'}, wait=True)
                conn.subscribe(destination=config_get('messaging-fts3', 'destination'),
                               ack='auto',)
                               #headers={'selector': 'vo = \'atlas\''})

        time.sleep(1)

    print 'consumer: graceful stop requested'

    for conn in conns:
        try:
            conn.disconnect()
        except:
            pass

    print 'consumer: graceful stop done'


def stop(signum=None, frame=None):
    """
    Graceful exit.
    """

    graceful_stop.set()


def run(once=False, process=0, total_processes=1, total_threads=1):
    """
    Starts up the messenger threads
    """

    print 'main: starting threads'
    threads = [threading.Thread(target=consumer, kwargs={'process': process, 'total_processes': total_processes, 'thread': i, 'total_threads': total_threads}) for i in xrange(0, total_threads)]

    [t.start() for t in threads]

    print 'main: waiting for interrupts'

    # Interruptible joins require a timeout.
    while len(threads) > 0:
        [t.join(timeout=3.14) for t in threads if t is not None and t.isAlive()]
