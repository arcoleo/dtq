"""
General include file
"""

import sys
import os
import logging
#import subprocess
#import MySQLdb
#import MySQLdb.cursors
import datetime
#from pprint import pprint
import operator
from operator import itemgetter

global log, rss_log_formatter, logger_handler, logger, LOGGING_INITIALIZED



def init_logging(log_module='unspecified'):
    """
    Initialize logging for a particular module.
    The log file file will be logs/log_module.log
    Since files may deal with communities, etc, there will be a master log 
    file for that file which will hold all the messages, a 'main' log file 
    with only the top level (non component) logging ending in .root.log, and 
    a log file for each component.
    """
    
    #try:
    #    log
    #except NameError:
    #    print 'New logging instance'
    #    pass
    #else:
    #    print 'Old logging instance'
    #    return
    #global log, log_formatter, rss_log_formatter, logger_handler, logger
    print 'dir', dir()
    # set to DEBUG, INFO, WARNING, ERROR, or CRITICAL
    try:
        foo
    except Exception, ex:
        print 'No foo'
    else:
        print 'FOOOO', foo
    try:
        logging.basicConfig(level=logging.DEBUG,
            format='[%(asctime)s] [%(levelname)s] [%(filename)s] \
    [%(funcName)s:%(lineno)d] %(message)s',
            datefmt='%a %b %d %H:%M:%S %Y',
            filename=log_module + '.log',
            filemode='a')
    except Exception, ex:
        print 'ERROR', ex
    console = logging.StreamHandler()
    # set to INFO, DEBUG, INFO, WARNING, ERROR, or CRITICAL
    console.setLevel(logging.DEBUG) # for msgs logged to console
    log_formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] \
[%(filename)s] [%(funcName)s:%(lineno)d] %(message)s')
    console.setFormatter(log_formatter)
    logging.getLogger('').addHandler(console)
    
    ret_rss_log_formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] \
[%(filename)s] [%(funcName)s:%(lineno)d] %(message)s')
    
    # log_master is the master log file for the script
    log_master = logging.getLogger('log')
    #log.info('Starting %s run...' % log_module)
    log_master.info('Starting %s run...' % log_module)
    
    # logger_hander is the dict that holds all the sub loggers
    ret_logger_handler = {}
    ret_logger = {}
    
    log_name = log_module + '.root'
    # 'log' is logger for the main part of the calling module, so messages not related to 
    # specific communities, etc can be seen
    ret_log = logging.getLogger(log_name)
    ret_logger_handler[1] = logging.FileHandler(log_name + '.log')
    ret_logger_handler[1].setFormatter(ret_rss_log_formatter)
    print ('log handlers', len(ret_log.handlers))
    if len(ret_log.handlers) < 1:
        ret_log.addHandler(ret_logger_handler[1])
        ret_log.debug('Starting Logging on new component')

    return ret_log, ret_rss_log_formatter, ret_logger_handler, ret_logger


def create_component_log(component_id):
    """
    Create a log file for a component so a master log doesn't get too cluttered.
    """

    timestamp = datetime.datetime.now().strftime('%Y-%m-%d')
    log_name = 'rss.' + str(component_id)
    logger[component_id] = logging.getLogger(log_name)
    logger_handler[component_id] = logging.FileHandler(
        timestamp + '.' + log_name + '.log')
    logger_handler[component_id].setFormatter(rss_log_formatter)
    logger[component_id].addHandler(logger_handler[component_id])
    logger[component_id].debug('Starting Logging on new component')


def get_dates(start_date_str,  end_date_str):
    """
    Returns an array of every day between two dates inclusive
    Input and output dates are 'YYYY-MM-DD'
    """
    
    log.debug(('Begin', ('start_date_str', start_date_str), 
        ('end_date_str', end_date_str)))
    start_date = datetime.datetime.strptime(start_date_str, '%Y-%m-%d')
    end_date = datetime.datetime.strptime(end_date_str, '%Y-%m-%d')
    #print start_date
    #print end_date
    date_array = []
    date_delta = (end_date + datetime.timedelta(days=1) - start_date).days
    for i in range(date_delta):
        date_array.append((start_date + 
            datetime.timedelta(days=i)).strftime('%Y-%m-%d'))
    log.debug('End')
    return date_array



def sort_table(table, cols):
    """
    sort a table by multiple columns
    table: a list of lists (or tuple of tuples) where each inner list 
        represents a row
    cols:  a list (or tuple) specifying the column numbers to sort by 
        e.g. (1,0) would sort by column 1, then by column 0
    """
    
    for col in reversed(cols):
        table = sorted(table, key=operator.itemgetter(col))
    return table


def sort_dict_by_size(param_dict):
    """
    Sort dictionary by size
    """
    
    sorted_list = {}
    for item in param_dict:
        sorted_list[item] = len(param_dict[item])
    print sorted_list
    # list of [(key, size), ...]
    return sorted(sorted_list.items(), key=itemgetter(1))


def safe_make_dir(target_dir, errstr='', fatal=False):
    log.debug('begin')
    #print ('safe_make_dir', 'begin', target_dir)
    try:
        os.makedirs(target_dir)
    except OSError, (errno, ex):
        if errno == 17:
            # already exists
            log.debug((target_dir, errno, ex))
            #print 'error', errno, target_dir, ex
        else:
            log.error((target_dir, 'OSError', errstr, ex))
            #print 'non 17 error', errno, target_dir, ex
            if fatal:
                sys.exit()
    except Exception, ex:
        log.error((errstr, ex))
        #print 'unknown error', ex
        if fatal:
            sys.exit()
    else:
        print 'success', target_dir
    log.debug('end')
            

if not hasattr(logging, 'set_up_done'):
    logging.set_up_done=False

try:
    LOGGING_INITIALIZED
except NameError:
    LOGGING_INITIALIZED = True
    if not logging.set_up_done:
        try:
            logging.set_up_done=True
            log, rss_log_formatter, logger_handler, logger = \
                init_logging('.'.join(os.path.abspath(os.path.splitext(
                    __file__)[0]).split(os.sep)[-2:]))
        except Exception, ex:
            print 'Logging Exception', ex 
