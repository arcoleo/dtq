#!/usr/bin/env python

import redis
from pprint import pprint
from optparse import OptionParser

def init_params():
    global options, redis_conn
    usage = './redis-queue --redis_server=<SERVER> [--clear]'
    parser = OptionParser(usage)
    parser.add_option('-v', '--verbose', action='store_true', dest='verbose')
    parser.add_option('-r', '--redis_server', action='store', dest='redis_server',
        default='localhost')
    parser.add_option('-c', '--clear', action='store_true', dest='clear',
        default=False)
    (options, args) = parser.parse_args()
    redis_conn = redis.Redis(options.redis_server)


def reset():
    if options.clear:
        print 'Resetting'
        for item in redis_conn.keys('job.*'):
            redis_conn.delete(item)
    else:
        print 'Not Resetting'

        
def show_status():
    if options.verbose:
        pprint(redis_conn.info())
        print '\nKeys'
        pprint(redis_conn.keys('job.*'))

    print 'Job Status'
    for item in redis_conn.keys('job.*'):
        print item, redis_conn.get(item)


if __name__ == '__main__':
    init_params()
    reset()
    show_status()
