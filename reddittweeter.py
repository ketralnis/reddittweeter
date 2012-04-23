#!/usr/bin/env python

import sys
import json
import time
import urllib2
from calendar import timegm
from datetime import datetime, timedelta
from itertools import chain
from xml.sax.saxutils import unescape as unescape_html

import tweepy, tweepy.error

from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, String, Integer, create_engine
from sqlalchemy.ext.declarative import declarative_base

debug = False

dbname = 'reddittweeter.db'
maxtweets = 10 # don't tweet more than this in one session
keepfor = timedelta(days=30) # how long to keep articles in the sqlite
                             # cache to keep them from being tweeted
                             # twice
opener = urllib2.build_opener()
opener.addheaders = [("User-agent", "reddittweeter")]

encoding = 'utf-8'
maxlength = 140

################################################################################

Base = declarative_base()

class Article(Base):
    __tablename__ = 'article'
    id = Column(String, primary_key = True)
    timestamp = Column(Integer, index=True)

    def __init__(self, id, timestamp):
        self.id = id
        self.timestamp = timestamp


def link_tokens(data):
    link = 'http://redd.it/%s' % data['id']

    tokens = [ unescape_html(data['title']),
               ' [%s]' % data['subreddit'],
               ' %d points' % data['score'],
               ', submitted by %s' % data['author'],
               ' [%s]' % data['domain'],
               ]
    return link, tokens


def comment_tokens(data):
    link = 'on http://redd.it/%s' % data['link_id'].split('_')[1]
    tokens = [ '"%s"' % unescape_html(data['body']),
               ', commented by %s' % data['author'],
               ]
    return link, tokens


def tweet_item(entry):
    kind = entry['kind']
    data = entry['data']

    if kind == 'Listing':
        for child in entry['data']['children']:
            for x in tweet_item(child):
                yield x

    else:
        if kind == 't1':
            link, tokens = comment_tokens(data)
        elif kind == 't3':
            link, tokens = link_tokens(data)
        else:
            raise ValueError("Unknown reddit type %r" % kind)

        fname = data['name']

        if data.get('over_18', False):
            return

        message_postfix = (' %s' % link).encode(encoding)

        tokens = [ t.encode(encoding) for t in tokens ]
        title, extras = tokens[0], tokens[1:]

        if len(title) + len(message_postfix) > maxlength:
            title = title[:maxlength-len(title)-len(message_postfix)-3]
            message = "%s...%s" % (title, message_postfix)
        else:
            # add all of the extra tokens that fit within the length
            # limit
            message = title + message_postfix
            for extra in extras:
                if len(message) + len(extra) < maxlength:
                    message += extra
                else:
                    break

        yield data['name'], message


def main(sourceurl, twitter_consumer, twitter_secret,
         twitter_access_key, twitter_access_secret):
    engine = create_engine('sqlite:///%s' % dbname, echo = debug)
    Session = sessionmaker(bind=engine)
    session = Session()
    Base.metadata.create_all(engine)

    auth = tweepy.OAuthHandler(twitter_consumer, twitter_secret)
    auth.set_access_token(twitter_access_key, twitter_access_secret)
    api = tweepy.API(auth)

    text = opener.open(sourceurl).read()
    parsed = json.loads(text)

    # there may be multiple listings, like on a comments-page
    if isinstance(parsed, dict):
        parsed = [parsed]
    assert isinstance(parsed, list)

    numtweets = 0

    for msg_id, message in chain.from_iterable(tweet_item(x) for x in parsed):
        existing = session.query(Article).filter_by(id = msg_id).first()
        if existing and debug:
            print "Skipping %r" % msg_id

        elif not existing:
            if numtweets > 0:
                # sleep between tweets so as not to hit them too hard
                time.sleep(1)

            if debug:
                print "Tweeting %r: %r" % (msg_id, message)

            try:
                api.update_status(message)
            except tweepy.error.TweepError, e:
                # selectively ignore duplicate tweet errors
                if 'duplicate' not in e.reason:
                    raise
                elif debug:
                    print "Warning: ignoring duplicate tweet"

            timestamp = timegm(datetime.now().timetuple())
            session.add(Article(msg_id, timestamp))
            session.commit() # commit after every item so that we
                             # don't tweet the same item twice, even
                             # if we throw an exception later on

            numtweets += 1
            if numtweets >= maxtweets:
                if debug:
                    print "Too many tweets (%d/%d). Quitting early" % (numtweets, maxtweets)
                break

    # clean up old db items to keep it from ballooning in size
    expiry = timegm((datetime.now() - keepfor).timetuple())
    session.query(Article).filter(Article.timestamp < expiry).delete()
    session.commit()


if __name__ == '__main__':
    if len(sys.argv) != 6:
        print "Usage: reddittweeter SOURCEURL TWITTER_CONSUMER TWITTER_SECRET ACCESS_KEY ACCESS_SECRET"
        sys.exit(1)
    sourceurl = sys.argv[1]
    # these are the tokens you get with your registered app
    twitter_consumer = sys.argv[2]
    twitter_secret   = sys.argv[3]
    # these are the credentials for the account doing the tweeting.  See
    #   http://joshthecoder.github.com/tweepy/docs/auth_tutorial.html
    twitter_access_key = sys.argv[4]
    twitter_access_secret = sys.argv[5]
    
    main(sourceurl, twitter_consumer, twitter_secret,
         twitter_access_key, twitter_access_secret)
