#!/usr/bin/python
import os
import pprint
import datetime
from twisted.internet import reactor, protocol, defer
from twisted.web import client
from twisted.enterprise import adbapi, util as dbutil
import feedparser, time, sys, cStringIO
from et import rss_feeds, expanded_dict_feeds	# (url, description)
import gi

rss_feeds = rss_feeds[1:5]

DEFERRED_GROUPS = 60	   # Number of simultaneous connections
INTER_QUERY_TIME = 300	   # Max Age (in seconds) of each feed in the cache
TIMEOUT = 30			   # Timeout in seconds for the web request
# dict cache's structure will be the following: { 'URL': (TIMESTAMP, value) }
cache = {  }
BASE_DIR = '/Users/arcoleo/Sites/rss_new/'
existing_feeds = {}
hash_path = {}
dbpool = adbapi.ConnectionPool("pyPgSQL.PgSQL", database="dsa_gooui", user="user", password="password")
total_new_articles = 0

class FeederProtocol(object):
	def __init__(self):
		self.parsed = 0
		self.error_list = [	 ]
		self.error_dict = {}
		self.timestamp = {}
		self.hash_path = {}
		self.db = dbpool
		self.res = 0
		self.condensed_file_path = {}
		self.condensed_data = {}
		
		if os.access(BASE_DIR + 'raw/', os.F_OK) == False:
			print 'New raw directory'
			try:
				os.mkdir(BASE_DIR + 'raw/')
			except Exception, ex:
				print 'Failed to create raw', ex
		if os.access(BASE_DIR + 'condensed/', os.F_OK) == False:
			print 'New condensed directory'
			try:
				os.mkdir(BASE_DIR + 'condensed/')
			except Exception, ex:
				print 'Failed to create condensed', ex

	def isCached(self, site):
		''' do we have site's feed cached (from not too long ago)? '''
		# how long since we last cached it (if never cached, since Jan 1 1970)
		elapsed_time = time.time( ) - cache.get(site, (0, 0))[0]
		return elapsed_time < INTER_QUERY_TIME
	def gotError(self, traceback, extra_args):
		''' an error has occurred, print traceback info then go on '''
		# extra_args appears to always be ('url', 'failed_function')
		print traceback, extra_args
		self.error_list.append(extra_args)
		# self.error_dict[url] = failed_function
		self.error_dict[extra_args[0]] = extra_args[1]
	def getFileInfo(self, data, addr):
		''' Find out what file to write to '''
		# get latest file belonging to this feed
		curr_hash = hash(addr)
		self.hash_path[addr] = {}
		self.hash_path[addr]['raw'] = BASE_DIR + 'raw/_' + str(curr_hash) + '/'
		self.hash_path[addr]['condensed'] = BASE_DIR + 'condensed/_' + str(curr_hash) + '/'
		curr_hash_path = self.hash_path[addr]['raw']
		max_feed = 0
		
		print 'data: ', data
		print 'addr: ', addr
				
		if os.access(self.hash_path[addr]['raw'], os.F_OK) == False:
			try:
				os.mkdir(self.hash_path[addr]['raw'])
			except Exception, ex:
				print 'Failed to raw create directory: ', ex
				return
		if os.access(self.hash_path[addr]['condensed'], os.F_OK) == False:
			try:
				os.mkdir(self.hash_path[addr]['condensed'])
			except Exception, ex:
				print 'Failed to create condensed directory: ', ex
				return

		file_path = self.hash_path[addr]['condensed'] + 'condensed.txt'
		try:
			fp = open(file_path, 'r')
		except IOError, ex:
			print 'Failed to open condensed.txt: ', ex
			# warning - could fail because of +w -r options.  this would overwrite
			try:
				fp = open(file_path, 'w')
			except IOError, ex:
				print 'Failed to create condensed.txt: ', ex
			else:
				fp.close()
		else:
			fp.close()

#		else:
#			try:
#				existing_feeds[addr] = os.listdir(curr_hash_path)
#			except Exception, ex:
#				print 'Failed to get directory contents: ', ex
#			else:
#				print 'Contents of ', curr_hash_path, ': ', existing_feeds[addr]
		print 'max_feed: ', max_feed
		
	def getPageFromMemory(self, data, addr):
		''' callback for a cached page: ignore data, get feed from cache '''
		return defer.succeed(cache[addr][1])
	def parseFeed(self, feed, addr):
		''' wrap feedparser.parse to parse a string '''
		if self.error_dict.get(addr) == 'getPage':
			return
		ts = datetime.datetime.utcnow()
		self.timestamp[addr] = str(ts.year) +  str(ts.month) +  \
			str(ts.day) + '-' + str(ts.hour) + \
			str(ts.minute) + str(ts.second)
		curr_hash = hash(addr)
		file_path = self.hash_path[addr]['raw'] + self.timestamp[addr] + '.xml'
		try:
			fp = open(file_path, 'w')
		except Exception, ex:
			print 'Failed to open file: ', ex
		else:
			fp.write(feed)
			fp.close()
		# print '\n\nparseFeedURL: ', addr
		try: feed+''
		except TypeError: feed = str(feed)
		try:
			parser_str = feedparser.parse(cStringIO.StringIO(feed))
		except Exception, ex:
			print '\n\nFailed to retieve URL: ', ex
		else:
			file_path = self.hash_path[addr]['raw'] + self.timestamp[addr] + '.txt'
			try:
				fp = open(file_path, 'w')
			except Exception, ex:
				print 'Failed to open file: ', ex
			else:
				fp.write(str(parser_str))
				#pprint.pprint(parser_str, fp)
				fp.close()
			#print 'Parsed Data: ', str(parser_str)
			return parser_str
	def memoize(self, feed, addr):
		''' cache result from feedparser.parse, and pass it on '''
		cache[addr] = time.time( ), feed
		return feed
	def condense_feeds(self, feed, addr):
		# load condensed.txt
		# read all txt files in raw
		# append to condensed dict
		# write back to condensed.txt
		
		# load condensed.txt
		self.condensed_file_path[addr] = self.hash_path[addr]['condensed'] + 'condensed.txt'
		try:
			fp = open(self.condensed_file_path[addr], 'r')
		except IOError, ex:
			print 'Failed to open condensed.txt: ', ex
			return feed
		self.condensed_data[addr] = fp.read()
		fp.close()
		# if condensed.txt is empty, dump current data to it
		if len(self.condensed_data[addr]) == 0:
			try:
				fp = open(self.condensed_file_path[addr], 'w')
			except IOError, ex:
				print 'Failed to write to condensed.txt', ex
				return feed
			fp.write(str(feed))
			fp.close()
		else:
			old_feed = eval(self.condensed_data[addr])
			print('\n\n\n\n\n')
			print 'Initial size:', len(old_feed['entries'])
			rss_diff = 0
			for newfeed_item in feed['entries']:
				if newfeed_item not in old_feed['entries']:
					old_feed['entries'].append(newfeed_item)
					rss_diff = 1
					total_new_articles += 1
			print 'New size:', len(old_feed['entries'])
			if rss_diff:
				try:
					fp = open(self.condensed_file_path[addr], 'w')
				except IOError, ex:
					print 'Failed to append to condensed.txt', ex
				else:
					fp.write(str(old_feed))
					fp.close()
#		print '\n\n\n\nCondensed Data\n', len(self.condensed_data[addr])
		return feed
	def async_send_to_db(self, feed, addr):
		self.sql_str = """Insert into rssfeed (url) values ('%s')""" % addr
		res = self.db.runOperation(self.sql_str)
		
		return feed
	def workOnPage(self, parsed_feed, addr):
		''' just provide some logged feedback on a channel feed '''
		if self.error_dict.get(addr) == 'getPage':
			return
		chan = parsed_feed.get('channel', None)
		if chan:
			print '\n\n\n******', addr, '\n'
			ptitle = chan.get('title', '(no channel title?)')

		return parsed_feed
	def stopWorking(self, data=None):
		''' just for testing: we close after parsing a number of feeds.
			Override depending on protocol/interface you use to communicate
			with this RSS aggregator server.
		'''
		print "Closing connection number %d..." % self.parsed
		
		self.parsed += 1
		print 'Parsed', self.parsed, 'of', self.END_VALUE
		print "=-"*20, '\n\n'
		
		# commit to db
		
		# build list of only new feed items
		
		# append saved dict(feed)
		
		if self.parsed >= self.END_VALUE:
			print "Closing all..."
			if self.error_list:
				print 'Observed', len(self.error_list), 'errors'
				for i in self.error_list:
					print i
			#print '\nData:\n', data
			reactor.stop( )
	def getPage(self, data, args):
		return client.getPage(args, timeout=TIMEOUT)
	def printStatus(self, data=None):
		print "Starting feed group..."
	def start(self, data=None, standalone=True):
		d = defer.succeed(self.printStatus( ))
		
		for feed_index, feed in enumerate(data):
			if self.isCached(feed):
				d.addCallback(self.getPageFromMemory, feed)
				d.addErrback(self.gotError, (feed, 'getting from memory'))
			else:
				#print 'ffffeed: ', feed
				# get file into to write to
				d.addCallback(self.getFileInfo, feed)
				d.addErrback(self.gotError, (feed, 'getting file info'))

				# not cached, go and get it from the web directly
				d.addCallback(self.getPage, feed)
				d.addErrback(self.gotError, (feed, 'getPage'))
				
				# once gotten, parse the feed and diagnose possible errors
				d.addCallback(self.parseFeed, feed)
				d.addErrback(self.gotError, (feed, 'parseFeed'))
				
				# put the parsed structure in the cache and pass it on
				d.addCallback(self.memoize, feed)
				d.addErrback(self.gotError, (feed, 'memoize'))
				
				# send to db
				#d.addCallback(self.async_send_to_db, feed)
				#d.addErrback(self.gotError, (feed, 'async_send_to_db'))
				
			d.addCallback(self.condense_feeds, feed)
			d.addErrback(self.gotError, (feed, 'condense_feeds'))
			
			# write function to convert ['one', 'two'] -> "{'one', 'two'}"
		
			# append rssentries to db

			
			# now one way or another we have the parsed structure, to
			# use or display in whatever way is most appropriate
			d.addCallback(self.workOnPage, feed)
			d.addErrback(self.gotError, (feed, 'workOnPage'))
			
			# for testing purposes only, stop working on each feed at once
			if standalone:
				d.addCallback(self.stopWorking)
				d.addErrback(self.gotError, (feed, 'stopWorking'))
		if not standalone:
			return d
class FeederFactory(protocol.ClientFactory):
	protocol = FeederProtocol( )
	def __init__(self, standalone=False):
		self.feeds = self.getFeeds( )
		self.standalone = standalone
		self.protocol.factory = self
		self.protocol.END_VALUE = len(self.feeds) # this is just for testing
		if standalone:
			self.start(self.feeds)
	def start(self, addresses):
		# Divide into groups all the feeds to download
		if len(addresses) > DEFERRED_GROUPS:
			url_groups = [[	 ] for x in xrange(DEFERRED_GROUPS)]
			for i, addr in enumerate(addresses):
				url_groups[i%DEFERRED_GROUPS].append(addr[0])
		else:
			url_groups = [[addr[0]] for addr in addresses]
		for group in url_groups:
			if not self.standalone:
				return self.protocol.start(group, self.standalone)
			else:
				self.protocol.start(group, self.standalone)
	def getFeeds(self, where=None):
		# used for a complete refresh of the feeds, or for testing purposes
		if where is None:
			return rss_feeds
		return None


def static_send_to_db():
	try:
		web.connect(dbn='postgres', user='user', pw='password', db='dsa_gooui')
	except Exception, ex:
		print 'Failed to connect to postgres: ', ex
		return		
	
	for curr_feed in expanded_dict_feeds.keys():
		print '\n\n\n-----\n',expanded_dict_feeds[curr_feed],'\n-----\n\n'
		print '\n\n>>>', curr_feed, '<<<\n'
		at_topic = []
		at_topic.append(expanded_dict_feeds[curr_feed].get('topic') or ' ')
		at_political = (expanded_dict_feeds[curr_feed].get('political') or ' ')
		at_topic_sports = (expanded_dict_feeds[curr_feed].get('topic_sports') or ' ')
		at_topic_entertainment = (expanded_dict_feeds[curr_feed].get('topic_entertainment') or ' ')
		at_url = (expanded_dict_feeds[curr_feed].get('xmlUrl') or ' ')
		
#		print 'Inserting url=%s, autotag_topic=%s, autotag_political=%s, autotag_topic_sports=%s, autotag_topic_entertainment=%s, processed=0' % (at_url, at_topic, at_political, at_topic_sports, at_topic_entertainment)
		#sql_str = "INSERT INTO rssfeed (url, autotag_topic) VALUES ('%s', '%s');" % (at_url, at_topic)
		print 'Insert:', sql_str,'\n'
		try:
			web.insert("rssfeed", seqname=False, url=at_url)#, autotag_topic=at_topic)
			#, autotag_political=at_political, autotag_topic_sports=at_topic_sports, autotag_topic_entertainment=at_topic_entertainment)
		except Exception, ex:
			print 'Failed to insert ', curr_feed, ': ', ex, '\n\n'
			sys.exit()

	
def do_main():
	global f
	f = FeederFactory(standalone=True)
	reactor.run()
	
	print 'Printed after everything has stopped'
	print 'Total new articles: ', total_new_articles
	#send_to_db()
	
if __name__=="__main__":
	do_main()

