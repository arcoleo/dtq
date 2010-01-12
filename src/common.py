#!/usr/bin/env python

import sys
import time
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.python import log
import txamqp
import redis

RABBIT_MQ_HOST = 'localhost'
RABBIT_MQ_PORT = 5672

VHOST = '/'
EXCHANGE_NAME = 'dtq_message_exchange'
QUEUE_NAME = 'dtq_message_queue'
ROUTING_KEY = 'dtq_routing_key'
CONSUMER_TAG = 'dtq_consumer_tag'

NON_PERSISTENT = 1
PERSISTENT = 2

credentials = {'LOGIN': 'guest', 'PASSWORD': 'guest'}


@inlineCallbacks
def getConnection(client):
    conn = yield client.connectTCP(RABBIT_MQ_HOST, RABBIT_MQ_PORT)
    yield conn.start(credentials)
    returnValue(conn)


@inlineCallbacks
def getChannel(conn):
    # create a new channel for sending messages
    chan = yield conn.channel(3)
    # open a virtual connection
    # channels are used so that heavy-weight TCPIP connections can be
    # used by multiple light-weight channels
    yield chan.channel_open()
    returnValue(chan)


def recv_callback(msg, chan, queue, redis_conn):
    print ('ack received', msg.content.body)
    # simulate work
    time.sleep(5)
    try:
        chan.basic_ack(msg.delivery_tag)
    except:
        print 'Error:', log.err()
    else:
        if redis_conn:
            redis_conn.set('job.' + msg.content.body, 'Done')
        return msg
    # if you uncomment the line below, comment out the while True
    # loop in consumer-slave.py:main
    # A side effect will be that nothing after queue.get() will be
    # called in consumer-slave.py:processMessage
    #return queue.get().addCallback(recv_callback, chan, queue)


def errorHandler(failure):
    #log.err()
    #print 'failure', str(failure), '\n\n\n'
    foo = failure.trap(txamqp.queue.Closed)
    if foo == txamqp.queue.Closed:
        print '\nClosed trap'
        return 'CLOSE'
    #print ('foo', foo)
    #('err', log.err())
    return None
    #sys.exit()
