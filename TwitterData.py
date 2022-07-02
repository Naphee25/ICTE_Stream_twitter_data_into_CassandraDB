from __future__ import print_function

import json
import time
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
import tweepy
import Keys


def connect_cassandra():
    auth_provider = PlainTextAuthProvider(username=Keys.cassandrauser, password=Keys.casspassword)
    cluster = Cluster([Keys.host_list], protocol_version=4,
                      auth_provider=auth_provider, port=Keys.cassandra_port)
    session = cluster.connect()
    session.set_keyspace('twittercassdb')
    return session


cursor_twittercassdb = connect_cassandra()


def create_tweets_table_cassandra(words):
    """
    This function open a connection with an already created database and creates a new table to
    store tweets related to a subject specified by the user
    """
    query_create = "CREATE TABLE IF NOT EXISTS %s(id UUID PRIMARY KEY, created_at text, tweet text, user_name text, user_id text, followers_count int,, friends_count int, status_count int, favorite_count int, lang text, quotecount int, replycount int, retweetcount int, favoritecount int, location text, retweeted boolean, favorited boolean );" % ("tweets_words")
    print(query_create)
    cursor_twittercassdb.execute(query_create)
    return


def store_tweets_in_table_cassandra(words, user_id, created_at, tweet, user_name, followers_count, friends_count, status_count, favorite_count, lang, quotecount, replycount,
                                      retweetcount, favoritecount, location,  retweeted, favorited):

    cursor_twittercassdb.execute("INSERT INTO %s (id,created_at, tweet, user_id, user_name, followers_count,friends_count, status_count, favorite_count, lang,quotecount, replycount, retweetcount, favoritecount, location,  retweeted, favorited) VALUES (now(), %%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s);"  % ("tweets_words"),
        (created_at, tweet, user_id, user_name, followers_count, friends_count, status_count, favorite_count, lang,quotecount, replycount,
                                      retweetcount, favoritecount, location,  retweeted, favorited))
    return


class Listener(tweepy.Stream):
    row_count = 0

    def on_data(self, raw_data):
        # Load the Tweet into the variable "data"
            data = json.loads(raw_data)
            print(raw_data)
            Listener.row_count = Listener.row_count + 1
            print(Listener.row_count)

            global words
            data = json.loads(raw_data)
            user_id = data['user']['id_str']
            created_at = data['created_at']
            tweet = data['text']
            user_name = data['user']['screen_name']
            followers_count = data['user']['followers_count']
            friends_count = data['user']['friends_count']
            status_count = data['user']['statuses_count']
            favorite_count = data['user']['friends_count']
            lang = data['user']['lang']
            quotecount = data['quote_count']
            replycount = data['reply_count']
            retweetcount = data['retweet_count']
            favoritecount = data['retweet_count']
            favorited = data['favorited']
            retweeted = data['retweeted']
            location = data['user']['location']


            # Store them in the Cassandra table
            store_tweets_in_table_cassandra(words, user_id, created_at, tweet, user_name, followers_count,
                                            friends_count, status_count, favorite_count, lang, quotecount, replycount,
                                            retweetcount, favoritecount, location, retweeted, favorited)
            Listener.row_count = Listener.row_count + 1
            if Listener.row_count == 100:
                exit()


    def on_connection_error(self, status_code):
        if status_code == 420:
            return False


if __name__ == '__main__':
    auth = tweepy.OAuthHandler(Keys.CONSUMER_KEY, Keys.CONSUMER_SECRET)
    auth.set_access_token(Keys.ACCESS_TOKEN, Keys.ACCESS_TOKEN_SECRET)
    api = tweepy.API(auth)

    words = ['#bigdata', '#AI', '#datascience', '#machinelearning', '#ml', '#iot']
    create_tweets_table_cassandra(words)
    stream_tweet = Listener(Keys.CONSUMER_KEY, Keys.CONSUMER_SECRET, Keys.ACCESS_TOKEN, Keys.ACCESS_TOKEN_SECRET)
    stream_tweet.filter(track=words)
