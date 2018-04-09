# -*- coding: utf-8 -*-
"""
Created on Wed Mar 20 18:44:16 2018

@author: siva
"""

from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
from textblob import TextBlob
from wordcloud import WordCloud, STOPWORDS
import matplotlib.pyplot as plt
import MySQLdb
import json
import re

stopwords = set(STOPWORDS)
searchWord = "Google"
consumer_key = "HKGogrRHiaBtd5qTkjp1dohK8"
consumer_secret = "apy7KSo9ah8Gl0MFtbDrsc2mdD70wWd1ywyB2Zj9hJXIbNdz31"
access_token = "2906311254-KCXyYgaqJyltyOJ7ogQpngUq1MfCibQA29DnqWO"
access_token_secret = "dbhPsXzIyEl16Epiurfj7X8CJUdBUqsG4IR7lFwfsnmTu"

conn = MySQLdb.connect("localhost","root","root","pythondb")
print("connected with :",conn)
c = conn.cursor()
conn.set_character_set('utf8mb4')
c.execute('SET NAMES utf8mb4;')
c.execute('SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci')
#c.execute('SET CHARACTER SET utf8;')
#c.execute('SET character_set_connection=utf8;')

emoji_pattern = re.compile("["
        u"\U0001F600-\U0001F64F"  # emoticons
        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
        u"\U0001F680-\U0001F6FF"  # transport & map symbols
        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                           "]+", flags=re.UNICODE)

def get_text_cleaned(tweet):
    text = tweet['text']

    slices = []
    #Strip out the urls.
    if 'urls' in tweet['entities']:
        for url in tweet['entities']['urls']:
            slices += [{'start': url['indices'][0], 'stop': url['indices'][1]}]

    #Strip out the hashtags.
    if 'hashtags' in tweet['entities']:
        for tag in tweet['entities']['hashtags']:
            slices += [{'start': tag['indices'][0], 'stop': tag['indices'][1]}]

    #Strip out the user mentions.
    if 'user_mentions' in tweet['entities']:
        for men in tweet['entities']['user_mentions']:
            slices += [{'start': men['indices'][0], 'stop': men['indices'][1]}]

    #Strip out the media.
    if 'media' in tweet['entities']:
        for med in tweet['entities']['media']:
            slices += [{'start': med['indices'][0], 'stop': med['indices'][1]}]

    #Strip out the symbols.
    if 'symbols' in tweet['entities']:
        for sym in tweet['entities']['symbols']:
            slices += [{'start': sym['indices'][0], 'stop': sym['indices'][1]}]

    # Sort the slices from highest start to lowest.
    slices = sorted(slices, key=lambda x: -x['start'])
    #print("slices ----",slices)
    #No offsets, since we're sorted from highest to lowest.
    for s in slices:
        text = text[:s['start']] + text[s['stop']:]

    text = emoji_pattern.sub(r'', text)
    text = ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)|(\W+)"," ",text).split())
    text = text.replace("RT", "")

    return text

def get_tweet_sentiment(tweet):
    '''
    Utility function to classify sentiment of passed tweet
    using textblob's sentiment method
    '''
    analysis = TextBlob(tweet)
    # set sentiment
    if analysis.sentiment.polarity > 0:
        return 'positive'
    elif analysis.sentiment.polarity == 0:
        return 'neutral'
    else:
        return 'negative'


class listener(StreamListener):

    def on_data(self, data):
        tweet = json.loads(data)
        parsed_tweet = {}

        # saving id_str of tweet
        parsed_tweet['id'] = tweet["id"]
        # saving screen_name of tweet
        parsed_tweet['screen_name'] = tweet["user"]["screen_name"]
        # saving search word
        parsed_tweet['search_word'] = searchWord
        # saving text of tweet
        parsed_tweet['text'] = get_text_cleaned(tweet)
        print(parsed_tweet['text'])
        # saving sentiment of tweet
        parsed_tweet['sentiment'] = get_tweet_sentiment(tweet['text'])
            #appending parsed tweet to tweets list
        c.execute("INSERT INTO alltweets (id, screen_name, search_word, tweet, sentiment) VALUES (%s,%s,%s,%s,%s)",
            (parsed_tweet["id"], parsed_tweet["screen_name"], parsed_tweet["search_word"], parsed_tweet["text"], parsed_tweet["sentiment"]))
        #print(parsed_tweet)
        print("above tweet save to db**************************")
        conn.commit()

        return True

    def on_error(self, status):
        print(status)

def streamToDb(consumer_key, consumer_secret, access_token, access_token_secret, searchWord):

    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    twitterStream = Stream(auth, listener())
    twitterStream.filter(track=[searchWord], languages=["en"])


def genWordCloud(text, title= None):

    wordcloud = WordCloud(
        background_color='white',
        stopwords=stopwords,
        max_words=200,
        max_font_size=40,
        scale=3,
        random_state=1 # chosen at random by flipping a coin; it was heads
    ).generate(str(text))

    fig = plt.figure(1, figsize=(12, 12))
    plt.axis('off')
    if title:
        fig.suptitle(title, fontsize=20)
        fig.subplots_adjust(top=2.3)

    plt.imshow(wordcloud)
    plt.show()

def fetchFromDb():

    c.execute("SELECT * FROM alltweets")
    numrows = c.rowcount
    allwords = ''
    for x in range(0,numrows):
        row = c.fetchone()
        #txtblb = TextBlob(str(row[3]), analyzer = NaiveBayesAnalyzer())
        print("***************************The tweets from DB*****************************")
        print(row[3])
        allwords += str(row[3])
    return allwords

def menu():
    print()
    print('Twitter Api menu')
    print('==================')
    print('1- To stream tweets to DB')
    print('2- To fetch tweets from DB')
    print('3- To Generate wordcloud')
    print('Q- Quit')
    menu_selected = input("Enter your choice : ")
    return menu_selected

def do_menu(menu_selected):
    #calling appropriate function based on the menu selection
    if menu_selected == '1':
        searchWord = input("Enter word to get tweets about..")
        streamToDb(consumer_key, consumer_secret, access_token, access_token_secret, searchWord)
    elif menu_selected == '2':
        alltweets = fetchFromDb()
    elif menu_selected == '3':
        alltweets = fetchFromDb()
        genWordCloud(alltweets)



def main():
    print()
    menu_selected = menu()
    #count = 0
    if menu_selected != 'Q':
        if menu_selected != 'q':
            do_menu(menu_selected)
    else:
        print("bye..")

if __name__ == "__main__":
    # calling main function
    main()
    
