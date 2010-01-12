#!/usr/bin/env python

import sys

from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet import reactor
from twisted.internet.protocol import ClientCreator

from txamqp.protocol import AMQClient
from txamqp.client import TwistedDelegate
from txamqp.content import Content
import txamqp.spec

import redis

from optparse import OptionParser

import common

@inlineCallbacks
def pushText(chan, body):
    msg = Content(body)
    msg['delivery mode'] = common.NON_PERSISTENT

    redis_conn.set('job.' + body, '--')

    # publish the message to our exchange
    # use the routing key to decide which quue the exchange should sent it to
    yield chan.basic_publish(exchange=common.EXCHANGE_NAME, content=msg,
        routing_key=common.ROUTING_KEY)
    returnValue(None)

@inlineCallbacks
def cleanUp(conn, chan):
    try:
        yield chan.channel_close()
    except txamqp.client.Closed:
        print 'Closed!'
    except Exception, ex:
        print sys.exc_info()
        reactor.stop()

    # AMQPClient creates an initial channel with id 0 when it first starts.
    # We get this channel so that we can close it
    chan = yield conn.channel(0)
    yield chan.connection_close()
    reactor.stop()
    returnValue(None)


@inlineCallbacks
def main(spec, content):
    delegate = TwistedDelegate()
    producer = ClientCreator(reactor, AMQClient, delegate=delegate,
        vhost=common.VHOST, spec=spec)
    conn = yield common.getConnection(producer)
    chan = yield common.getChannel(conn)
    yield pushText(chan, content)
    # shut down
    yield cleanUp(conn, chan)


def init_params():
    global options, redis_conn
    usage = "./publisher-client.py --xml=<xml_path> --amqp_server=<host> --redis_server <content>"
    parser = OptionParser(usage)
    parser.add_option('-v', '--verbose', action='store_true', dest='verbose')
    parser.add_option('-a', '--amqp_server', action='store', dest='amqp_server')
    parser.add_option('-x', '--xml', action='store', dest='xml',
        default='../xml/amqp0-8.xml')
    parser.add_option('-r', '--redis_server', action='store_true', dest='redis_server',
        default=False)
    (options, args) = parser.parse_args()
    if not options.xml:
        print 'Missing amqp xml spec'
        sys.exit(1)
    if not args:
        print 'Missing content'
        sys.exit(2)
    if options.amqp_server:
        common.RABBIT_MQ_HOST = options.amqp_server
    redis_conn = None
    if options.redis_server:
        redis_conn = redis.Redis('arcoleo.igb.uiuc.edu')
    return args[0]


if __name__ == '__main__':
    content = init_params()
    spec = txamqp.spec.load(options.xml)
    main(spec, content)
    reactor.run()
