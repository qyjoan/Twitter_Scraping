import os
import sys
import tweepy
import datetime
import urllib
import signal
import json
import partitions as par

class TweetSerializer:
   out = None
   first = True
   count = 0
   def start(self,d):
      self.count += 1
      fname = "/Users/joanqiu/MIDS/W205/week2/out/"+"tweets-"+d+".json"
      self.out = open(fname,"w")
      self.out.write("[\n")
      self.first = True

   def end(self):
      if self.out is not None:
         self.out.write("\n]\n")
         self.out.close()
      self.out = None

   def write(self,tweet):
      if not self.first:
         self.out.write(",\n")
      self.first = False
      self.out.write(json.dumps(tweet._json).encode('utf8'))


def interrupt(signum, frame):
   print "Interrupted, closing ..."
   # call ts.end when interrupt to ensure resilience
   ts.end()
   exit(1)

signal.signal(signal.SIGINT, interrupt)


consumer_key = os.environ['TWTCONSKEY'];
consumer_secret = os.environ['TWTCONSEC'];

access_token = os.environ['ACCTOKEN'];
access_token_secret = os.environ['ACCTOKSEC'];

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth_handler=auth,wait_on_rate_limit=True,wait_on_rate_limit_notify=True)

q = urllib.quote_plus(sys.argv[1])  # URL encoded query

# Additional query parameters:
#   since: {date}
#   until: {date}
# Just add them to the 'q' variable: q+" since: 2014-01-01 until: 2014-01-02"

# allowing a max number of tweets per file #

#tweet_limit = int(sys.argv[2])
#tweet_cnt = 0
#ts = TweetSerializer()
#ts.start()
#for tweet in tweepy.Cursor(api.search,q=q).items(100):
#   if tweet_cnt < tweet_limit:
#      ts.write(tweet)
#      tweet_cnt += 1
#   else:
#      ts.end()
#      ts.start()
#      ts.write(tweet)
#      tweet_cnt = 1
#ts.end()
   # FYI: JSON is in tweet._json

# partition data on facet

start_dt = sys.argv[2]
end_dt = sys.argv[3]
start = datetime.datetime.strptime(start_dt, par.xsdDateFormat)
end = datetime.datetime.strptime(end_dt, par.xsdDateFormat)

for strd in par.date_partition(start, end):
   ts = TweetSerializer()
   start_dt = str(strd).split(' ')[0]
   end_dt = str(strd + datetime.timedelta(days=1)).split(' ')[0]
   ts.start(start_dt)
   for tweet in tweepy.Cursor(api.search, q = q, since = start_dt, until = end_dt).items(10):
      ts.write(tweet)
   ts.end()
