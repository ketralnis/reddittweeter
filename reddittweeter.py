#!/usr/local/bin/python

import re
import sys
import time
import twitter
import feedparser
from calendar import timegm
from datetime import datetime, timedelta

from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, String, Integer, create_engine
from sqlalchemy.ext.declarative import declarative_base

debug = False

sourceurl = "http://www.reddit.com/.rss"
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

def db_maintenance(metadata):
    pass

def main(username, password):
    engine = create_engine('sqlite:///%s' % dbname, echo = debug)
    Session = sessionmaker(bind=engine)
    session = Session()
    Base.metadata.create_all(engine)

    api = twitter.Api(username=username, password=password, input_encoding=encoding)

    feed = feedparser.parse(sourceurl)

    urlanalyser = re.compile('http://www.reddit.com/r/([.a-zA-z0-9-_]+)/comments/([a-zA-z0-9]+)/.*')
    numtweets = 0

    for entry in feed.entries:
        match = urlanalyser.match(entry.link)
        sr = str(match.group(1))
        link_id = str(match.group(2))
        timestamp = timegm(entry.updated_parsed)

        existing = session.query(Article).filter_by(id = link_id).first()
        if existing and debug:
            print "Skipping %r" % entry.title
        elif not existing:
            title = entry.title.strip().encode(encoding)
            message_postfix = " http://reddit.com/%s" % link_id

            if len(title) + len(message_postfix) > maxlength:
                title = title[:maxlength-len(title)-len(message_postfix)-3]
                message = "%s...%s" % (title, message_postfix)
            else:
                message = "%s%s" % (title, message_postfix)

            if debug:
                print "Tweeting %r" % message

            api.PostUpdate(message)
            time.sleep(1) # don't hit them too hard

            session.add(Article(link_id, timestamp))
            session.commit()

            numtweets += 1
            if numtweets >= maxtweets:
                if debug:
                    print "Quitting early"
                break

    expiry = timegm((datetime.now() - keepfor).timetuple())
    old_ids = session.query(Article).filter(Article.timestamp < expiry).delete()

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print "Usage: redditweeter USERNAME PASSWORD"
        sys.exit(1)
    username = sys.argv[1]
    password = sys.argv[2]
    main(username, password)
