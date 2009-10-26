#!/usr/local/bin/python

import sys
import json
import time
import urllib
from calendar import timegm
from datetime import datetime, timedelta
from xml.sax.saxutils import unescape as unescape_html

import twitter

from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, String, Integer, create_engine
from sqlalchemy.ext.declarative import declarative_base

debug = False

dbname = 'reddittweeter.db'
maxtweets = 10 # don't tweet more than this in one session
keepfor = timedelta(days=7) # how long to keep articles in the sqlite
                            # cache to keep them from being tweeted
                            # twice

encoding = 'utf-8'
maxlength = 140

################################################################################

Base = declarative_base()

class Article(Base):
    __tablename__ = 'article'
    id = Column(String, primary_key = True)
    timestamp = Column(Integer)

    def __init__(self, id, timestamp):
        self.id = id
        self.timestamp = timestamp


def link_tokens(data):
    link = 'http://reddit.com/%s' % data['id']

    tokens = [ unescape_html(data['title']),
               ' [%s]' % data['subreddit'],
               ' %d points' % data['score'],
               ', submitted by %s' % data['author'],
               ' [%s]' % data['domain'],
               ]
    return link, tokens


def comment_tokens(data):
    link = 'on http://reddit.com/%s' % data['link_id'].split('_')[1]
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

        message_postfix = (' %s' % link).encode(encoding)

        tokens = [ t.encode(encoding) for t in tokens ]
        title, extras = tokens[0], tokens[1:]

        if len(title) + len(message_postfix) > maxlength:
            title = title[:maxlength-len(title)-len(message_postfix)-3]
            message = "%s...%s" % (title, message_postfix)
        else:
            # add all of the extra tokens that fit within the length
            # limit
            for extra in extras:
                if len(title) + len(extra) + len(message_postfix) < maxlength:
                    title += extra
                else:
                    break
            message = "%s%s" % (title, message_postfix)

            yield data['name'], message


def _flatiter(x):
    for y in x:
        for z in y:
            yield z


def main(sourceurl, twitter_username, twitter_password):
    engine = create_engine('sqlite:///%s' % dbname, echo = debug)
    Session = sessionmaker(bind=engine)
    session = Session()
    Base.metadata.create_all(engine)

    api = twitter.Api(username=twitter_username, password=twitter_password,
                      input_encoding=encoding)

    text = urllib.urlopen(sourceurl).read()
    parsed = json.loads(text)

    # there may be multiple listings, like on a comments-page
    if isinstance(parsed, dict):
        parsed = [parsed]
    assert isinstance(parsed, list)

    numtweets = 0

    for msg_id, message in _flatiter(tweet_item(x) for x in parsed):

        existing = session.query(Article).filter_by(id = msg_id).first()
        if existing and debug:
            print "Skipping %r" % msg_id

        elif not existing:
            if debug:
                print "Tweeting %r: %r" % (msg_id, message)

            api.PostUpdate(message)
            time.sleep(1) # don't hit them too hard

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


if __name__ == '__main__':
    if len(sys.argv) != 4:
        print "Usage: reddittweeter SOURCEURL TWITTER_USERNAME TWITTER_PASSWORD"
        sys.exit(1)
    sourceurl = sys.argv[1]
    twitter_username = sys.argv[2]
    twitter_password = sys.argv[3]
    main(sourceurl, twitter_username, twitter_password)
