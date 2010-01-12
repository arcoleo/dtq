#!/usr/bin/env python

import sys

from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet import reactor
from twisted.internet.protocol import ClientCreator
from twisted.python import log

from txamqp.client import Closed

from txamqp.protocol import AMQClient
from txamqp.client import TwistedDelegate
import txamqp.spec

from optparse import OptionParser
import redis

import common

DURABLE=True
AUTO_DELETE=False
NO_ACK=False

@inlineCallbacks
def getQueue(conn, chan):
    # create an exchange on the message server
    yield chan.exchange_declare(
        exchange=common.EXCHANGE_NAME,
        type='direct',
        durable=DURABLE,
        auto_delete=AUTO_DELETE)

    # create a message queue on the messager server
    yield chan.queue_declare(
        queue=common.QUEUE_NAME,
        durable=DURABLE,
        exclusive=False,
        auto_delete=AUTO_DELETE)

    # bind the exhange and the message queue
    yield chan.queue_bind(
        queue=common.QUEUE_NAME,
        exchange=common.EXCHANGE_NAME,
        routing_key=common.ROUTING_KEY)

    # create a consumer
    yield chan.basic_consume(
        queue=common.QUEUE_NAME,
        consumer_tag=common.CONSUMER_TAG,
        no_ack=NO_ACK)

    # get the queue that's associated with our consumer
    queue = yield conn.queue(common.CONSUMER_TAG)
    returnValue(queue)


@inlineCallbacks
def processMessage(conn, chan, queue):
    try:
        print 'Listening Consumer...'
        msg = yield queue.get().addCallbacks(
            common.recv_callback,
            callbackArgs=(chan, queue, redis_conn),
            errback=common.errorHandler)
        if msg == 'CLOSE':
            returnValue(False)
    except Exception, ex:
        print 'Queue closed', ex
    else:
        # msg will not be assigned here if addCallback from recv_callback
        # is uncommented
        print "Received: %s from channel #%s" % (msg.content.body, chan.id)
    returnValue(True)


@inlineCallbacks
def main(spec):
    delegate = TwistedDelegate()
    consumer = ClientCreator(reactor, AMQClient, delegate=delegate,
        vhost=common.VHOST, spec=spec)
    conn = yield common.getConnection(consumer)
    chan = yield common.getChannel(conn, common.IS_SERVER)
    queue = yield getQueue(conn, chan)
    loop = True
    while loop:
        loop = yield processMessage(conn, chan, queue)


def init_params():
    global options, redis_conn
    usage = "./consumer-slave.py --xml=<xml_path> --amqp_server=<host>"
    parser = OptionParser(usage)
    parser.add_option('-v', '--verbose', action='store_true', dest='verbose')
    parser.add_option('-a', '--amqp_server', action='store', dest='amqp_server')
    parser.add_option('-r', '--redis_server', action='store_true', dest='redis_server',
        default=False)
    parser.add_option('-x', '--xml', action='store', dest='xml',
        default='../xml/amqp0-8.xml')
    (options, args) = parser.parse_args()
    if not options.xml:
        print 'Missing amqp xml spec'
        sys.exit(1)
    if options.amqp_server:
        common.RABBIT_MQ_HOST = options.amqp_server
    common.IS_SERVER = options.server
    redis_conn = False
    if options.redis_server:
        redis_conn = redis.Redis('arcoleo.igb.uiuc.edu')

if __name__ == '__main__':
    init_params()
    spec = txamqp.spec.load(options.xml)
    main(spec)
    reactor.run()

