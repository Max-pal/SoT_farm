# Extracting streaming data from Twitter, pre-processing, and loading into Heroku PSQL
import credentials  # Import api/access_token keys from credentials.py
import settings  # Import related setting constants from settings.py
import psycopg2
import re
import tweepy
from textblob import TextBlob


# Streaming With Tweepy
class MyStreamListener(tweepy.StreamListener):

    def on_status(self, status):
        # Extract info from tweets
        if status.retweeted:
            # Avoid re-tweeted info, and only original tweets will be received
            return True
        # Extract attributes from each tweet
        id_str = status.id_str
        created_at = status.created_at
        text = deEmojify(status.text)  # Pre-processing the text
        client_source = status.source
        sentiment = TextBlob(text).sentiment # Data analysis
        polarity = sentiment.polarity
        subjectivity = sentiment.subjectivity
        user_created_at = status.user.created_at
        user_location = deEmojify(status.user.location)
        # user_geo_preference = status.geo_enabled
        user_lang = status.lang
        user_description = deEmojify(status.user.description)
        user_followers_count = status.user.followers_count
        # user_place = status.places
        # retweet_count = status.retweet_count
        # favorite_count = status.favorite_count

        # Store all data in Heroku PSQL
        cur = conn.cursor()
        sql = "INSERT INTO {} (id_str, created_at, text, polarity, subjectivity, user_created_at, user_location, user_description, user_followers_count, user_lang, client_source) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s ,%s)".format(
            settings.TABLE_NAME)
        val = (id_str, created_at, text, polarity, subjectivity, user_created_at, user_location, \
               user_description, user_followers_count, user_lang, client_source)
        cur.execute(sql, val)
        conn.commit()
        # use dictonary instead of tupple
        # delete_query = '''
        # DELETE FROM {0}
        # WHERE id_str IN (
        #     SELECT id_str
        #     FROM {0}
        #     ORDER BY created_at asc
        #         LIMIT 200) AND (SELECT COUNT(*) FROM {0}) > 9600;
        # '''.format(settings.TABLE_NAME)
        #
        # cur.execute(delete_query)
        # conn.commit()
        # cur.close()

    def on_error(self, status_code):
        '''
        Stop scrapping data as it exceed to the threshold.
        '''
        if status_code == 420:
            # return False to disconnect the stream
            return False


def clean_tweet(self, tweet):
    ''' 
    Clean tweet text by removing links and special characters
    '''
    return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t]) \
                                |(\w+:\/\/\S+)", " ", tweet).split())


def deEmojify(text):
    '''
    Strip all non-ASCII characters to remove emoji characters
    '''
    if text:
        return text.encode('ascii', 'ignore').decode('ascii')
    else:
        return None


                #TheMandaloiran table 'postgres://mrhohhmmvyfame:6932cd6eaa940cc6adee6faabea5e292c57dd7a87614626e037ef8206067eb66@ec2-54-75-224-168.eu-west-1.compute.amazonaws.com:5432/de3gatit5ecn4s'
DATABASE_URL = 'postgres://dbmasteruser:W?SbU5f6MOS9CG2z1C*C)hz?)>SF|2l,@ls-32cbb4d180aaad0b359695120eedc91e6fca9dfa.clcnyrcuavnn.eu-central-1.rds.amazonaws.com:5432/postgres' \

conn = psycopg2.connect(DATABASE_URL, sslmode='require')
cur = conn.cursor()

cur.execute("""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = '{0}'
        """.format(settings.TABLE_NAME))
if cur.fetchone()[0] == 0:
    cur.execute("CREATE TABLE {} ({});".format(settings.TABLE_NAME, settings.TABLE_ATTRIBUTES))
    conn.commit()
cur.close()

auth = tweepy.OAuthHandler(credentials.API_KEY, credentials.API_SECRET_KEY)
auth.set_access_token(credentials.ACCESS_TOKEN, credentials.ACCESS_TOKEN_SECRET)
api = tweepy.API(auth)

myStreamListener = MyStreamListener()
myStream = tweepy.Stream(auth=api.auth, listener=myStreamListener)
myStream.filter(languages=["en"], track=settings.TRACK_WORDS)
conn.close()
