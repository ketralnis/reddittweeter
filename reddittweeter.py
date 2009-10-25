#!/usr/local/bin/python

import sys
import json
import time
import urllib
import twitter
from calendar import timegm
from datetime import datetime, timedelta

from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, String, Integer, create_engine
from sqlalchemy.ext.declarative import declarative_base

debug = False

sourceurl = "http://www.reddit.com/.json"
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

def main(username, password):
    engine = create_engine('sqlite:///%s' % dbname, echo = debug)
    Session = sessionmaker(bind=engine)
    session = Session()
    Base.metadata.create_all(engine)

    api = twitter.Api(username=username, password=password, input_encoding=encoding)

    text = urllib.urlopen(sourceurl).read()
    parsed = json.loads(text)

    numtweets = 0

    for entry in parsed['data']['children']:
        data = entry['data']

        link_id = str(data['id'])
        sr = str(data['subreddit'])
        submitter = str(data['author'])
        timestamp = int(data['created'])
        score = int(data['score'])
        domain = str(data['domain'])

        try:
            title = str(data['title'])
        except UnicodeEncodeError:
            title = data['title'].encode(encoding)
        title = title.strip()

        existing = session.query(Article).filter_by(id = link_id).first()
        if existing and debug:
            print "Skipping %r" % title
        elif not existing:
            message_postfix = " http://reddit.com/%s" % link_id

            if len(title) + len(message_postfix) > maxlength:
                title = title[:maxlength-len(title)-len(message_postfix)-3]
                message = "%s...%s" % (title, message_postfix)
            else:
                # try to add some extra stuff (like the subreddit) to
                # the text if it fits
                extras = [' [%s]' % sr,
                          ' %d points' % score,
                          ', submitted by %s' % submitter,
                          ' [%s]' % domain]
                for extra in extras:
                    if len(title) + len(extra) + len(message_postfix) < maxlength:
                        title += extra
                    else:
                        break
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
