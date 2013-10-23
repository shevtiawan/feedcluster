#!/usr/bin/python
# encoding: utf-8
"""
reader.py

Created by Andri Setiawan on 2013-10-17.
Copyright (c) 2013 __MyCompanyName__. All rights reserved.
"""

import feedparser
import webbrowser
import mechanize
import os
import csv
import redis
from BeautifulSoup import BeautifulSoup, NavigableString
import HTMLParser
import re
import nltk
from nltk.tokenize import RegexpTokenizer
from nltk import bigrams, trigrams
from time import sleep
import time


class Engine:
    tokenizer = RegexpTokenizer("[\w']+", flags=re.UNICODE)

    def __init__(self):
        pass

    #notused
    def strip_tags(self,html, invalid_tags):
        soup = BeautifulSoup(html)
        for tag in soup.findAll(True):
            if tag.name in invalid_tags:
                s = ""

                for c in tag.contents:
                    if not isinstance(c, NavigableString):
                        c = self.strip_tags(unicode(c), invalid_tags)
                    s += unicode(c)

                tag.replaceWith(s)
        return soup

    def sanitize_html(self, value):
        soup = BeautifulSoup(value)
        valid_tags = ['p']

        for tag in soup.findAll(True):
            if tag.name not in valid_tags:
                tag.hidden = True
        return soup.renderContents()

    def clean_text(self,text):
        h = HTMLParser.HTMLParser()
        text = h.unescape(text)
        text = self.sanitize_html(text)
        #text = BeautifulSoup(text, convertEntities=BeautifulSoup.HTML_ENTITIES)        
        #text = text.replace("&nbsp;", " ")
        #text = text.replace("&amp;", " ")
        #text = text.replace("quot;", " ")
        return text.strip()

    def tokenize(self, text):
        stopwords_file = open('stopwords.txt',"r")
        stop_words = stopwords_file.readlines()
        stopwords_file.close()

        tokens = self.tokenizer.tokenize(text.lower())
        tokens = [token for token in tokens if token not in stop_words[0].split()]
        return tokens


class RSSReader:
    def __init__(self):
        self.feed_urls = []
        self.feed_sources = []
        self.feeds = []
        self.prepareFeedSources()

    def prepareFeedSources(self):
        feed_file = open('urllist.txt',"r")
        csv_reader = csv.reader(feed_file)
        for row in csv_reader:
            self.feed_sources.append(row[0].strip())
            self.feed_urls.append(row[1].strip())
        feed_file.close()
        
    def getFeeds(self):
        for url in self.feed_urls:
            feed = feedparser.parse(url)
            self.feeds.append(feed)

    def archiveFeeds(self):
        r = redis.StrictRedis(host='localhost', port=6379, db=0)
        engine = Engine()

        i = 0
        for feed in self.feeds:
            # source = feed["channel"]["title"]
            source = self.feed_sources[i]
            item_archived = 0
            i += 1
            for item in feed["items"]:
                image_url = ""
                for link in item.links:
                    if link.type == "image/jpg":
                        image_url = link.href

                summary_clean = engine.clean_text(item["description"])
                title_clean = engine.clean_text(item["title"])
                
                tokenize_summary = engine.tokenize(summary_clean)   # []
                print tokenize_summary
                tokenize_summary = ",".join(tokenize_summary)       # string
                tokenize_title = engine.tokenize(title_clean)       # []
                print tokenize_title
                tokenize_title = ",".join(tokenize_title)           # string

                
                tokenize_all = tokenize_title + "," + tokenize_summary

                feed_data = {
                    "source"        : source,
                    "published"     : item["published"],
                    "url"           : item["link"],
                    "title"         : item["title"],
                    "summary_raw"   : item["description"],
                    "image_url"     : image_url,
                    "full_text"     : " ", #todo:web content scrapper
                    "title_clean"   : title_clean,
                    "summary_clean" : summary_clean,
                    "tokens"        : tokenize_all
                }

                """
                key = "feed:" + item["link"]
                if not r.hexists(key,"source"):
                    r.sadd("urls:" + source, item["link"])
                    if r.sadd("urls", item["link"]):
                        if r.hmset(key,feed_data):
                            item_archived += 1
                """

                """
                if r.sadd("urls", item["link"]):
                    feed_key = r.incr("feed_id")
                    r.sadd("urls:" + source, feed_key)
                    r.hmset("feed:" + str(feed_key),feed_data)
                    item_archived += 1
                """
                if r.sadd("urls", item["link"]):
                    feed_key = r.incr("feed_id")
                    r.zadd("feeds:" + source, time.time(), feed_key)
                    r.zadd("feeds:", time.time(), feed_key)
                    r.hmset("feed:" + str(feed_key),feed_data)
                    item_archived += 1
                    
            print str(item_archived) + " " + source + " items archived."
        self.feeds = []
    
if __name__ == "__main__":
    r = redis.StrictRedis(host='localhost', port=6379, db=0)
    reader = RSSReader()
    while True:
        try:
            reader.getFeeds()
            print "archiving..."
            reader.archiveFeeds()
            print "done."
        except Exception, e:
            #raise e
            print "error:"
            print e
            print "resuming..."
        
        sleep(5 * 60) # seconds
    