'''
harry durbin
project to scrape and analyze tweets in chile
'''

import pandas as pd
import tweepy
from sqlalchemy.exc import ProgrammingError
import json
import dataset
from textblob import TextBlob
import private
import psycopg2
import sqlalchemy
import sys


enginestring = 'postgresql://harry:harry@localhost:5432/chileantweets'.format(private.user, private.password)
engine = sqlalchemy.create_engine(enginestring)

# # CONNECTION_STRING = "sqlite:///chile_tweets.db "
# TABLE_NAME = "chiletweets"

# db = dataset.connect(CONNECTION_STRING)

# Go to http://apps.twitter.com and create an app.
# The consumer key and secret will be generated for you after
TWITTER_APP_KEY = private.twitter_app_key
TWITTER_APP_SECRET = private.twitter_app_secret

# After the step above, you will be redirected to your app's page.
# Create an access token under the the "Your access token" section
TWITTER_KEY = private.twitter_key
TWITTER_SECRET = private.twitter_secret

class StreamListener(tweepy.StreamListener):

    i = 0

    def on_status(self, status):

        if status.coordinates != None:
            self.i+=1
            # print 'Tweet # {}'.format(self.i)

            coords = status.coordinates
            lng = coords[u'coordinates'][0]
            lat = coords[u'coordinates'][1]
            created = status.created_at
            tweetid = status.id
            idstr = status.id_str
            text = status.text
            userid = status.user.id
            name = status.user.name
            loc = status.user.location
            descr = status.user.description
            screenname = status.user.screen_name
            followers = status.user.followers_count
            retweets = status.retweet_count
            blob = TextBlob(text)
            sent = blob.sentiment

            atweet = [dict(
                            tweetid=tweetid,
                            created=created,
                            lng = lng,
                            lat = lat,
                            text=text,
                            userid=userid,
                            name=name,
                            loc=loc,
                            description=descr,
                            screenname=screenname,
                            follow=followers,
                            retweets=retweets,
                            polarity=sent.polarity,
                            subjectivity=sent.subjectivity
                            )
                    ]

            tweet_geo = [dict(tweetid = tweetid, lng = lng, lat = lat)]

            df = pd.DataFrame(atweet)
            df.set_index('tweetid',inplace=True)
            dfgeo = pd.DataFrame(tweet_geo)
            dfgeo.set_index('tweetid',inplace=True)
            dfgeo['lat'] = dfgeo['lat'].astype(float)
            dfgeo['lng'] = dfgeo['lng'].astype(float)
            dftemp = pd.DataFrame()
            dftemp['lat'] = dfgeo['lat'].values
            dftemp['lng'] = dfgeo['lng'].values

            try:

                dftemp.to_sql('tempor', engine, if_exists='replace')

                conn = psycopg2.connect("dbname=chileantweets user=harry host=localhost password='harry'")
                cur = conn.cursor()

                # ####### change to if_exists to replace' if initializing db
                # df.to_sql('chileantweets', engine, if_exists='replace')
                # dfgeo.to_sql('chileantweetsgeo', engine, if_exists='replace')

                ####### uncomment this if initializing database
                cur.execute("""
                SELECT AddGeometryColumn ('tempor', 'location', 4326, 'POINT', 2);
                UPDATE tempor
                SET location = ST_SetSRID (ST_Point(lng, lat), 4326);
                """
                )
                conn.commit()

                cur.execute("""
                SELECT COUNT(*)
                FROM tempor , chile
                WHERE ST_Contains(chile.geom, tempor.location);""")
                for record in cur:
                    tempvalue = record[0]
                cur.execute("""DROP TABLE tempor;""")

                if tempvalue == 1:

                    ##### change to if_exists to replace' if initializing db
                    df.to_sql('chileantweets', engine, if_exists='append')
                    dfgeo.to_sql('chileantweetsgeo', engine, if_exists='append')

                    cur.execute("""
                                UPDATE chileantweetsgeo
                                SET location = ST_SetSRID (ST_Point(lng, lat), 4326);
                                """
                                )

                    cur.execute("""
                    SELECT COUNT(*)
                    FROM chileantweetsgeo;
                    """)
                    for record in cur:
                        qty_total = record[0]
                    print 'Tweet number {} added!'.format(qty_total)
                    conn.commit()

                else:
                    print 'Tweet located outside of Chile.'
                    conn.commit()

                # if self.i % 500 == 0:
                #     conn.close()
                #     sys.exit()

                    # ############uncomment this if initializing database
                    # conn.close()
                    # sys.exit()


            except ProgrammingError as err:
                print(err)

    def on_error(self, status_code):
        if status_code == 420:
            #returning False in on_data disconnects the stream
            return False

auth = tweepy.OAuthHandler(TWITTER_APP_KEY, TWITTER_APP_SECRET)
auth.set_access_token(TWITTER_KEY, TWITTER_SECRET)
api = tweepy.API(auth)

stream_listener = StreamListener()
stream = tweepy.Stream(auth=api.auth, listener=stream_listener)
# filtering tweets in vicinity of chile
stream.filter(locations=[-77.42, -56.07, -65.29, -18.43])

    #
    # cur.execute("""
    # SELECT COUNT(*)
    # FROM chileantweetsgeo , chile
    # WHERE ST_Contains(chile.geom, chileantweetsgeo.location);""")
    # for record in cur:
    #     qty_in_chile = record[0]
    # # print qty_in_chile


# cur.execute(
#     '''
#     SELECT CAST(stop_route.stop_id AS INT), stop_geo.lat, stop_geo.lng
#     FROM stop_route
#     JOIN stop_geo
#     ON stop_route.stop_id = stop_geo.stop_id
#     WHERE stop_route.route_id = %s;
#     '''
#     , [int(df_route[df_route['route_name']==routenum].index[0])])
