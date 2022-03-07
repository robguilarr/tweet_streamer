# -------------------------------------------------------------------------------------------------------------------------------
# List of dependecies to run the Stream Listener
# -------------------------------------------------------------------------------------------------------------------------------

import tweepy # Associate connection to Twitter API Endpoints
import json # To manage transformation for tweepy status objects
import pandas as pd 
import sys 
from tweepy import Stream # To use instead of StreamListener for version 4.4.0

# -------------------------------------------------------------------------------------------------------------------------------
# Listener class 
# -------------------------------------------------------------------------------------------------------------------------------
#   This class is used to insert a tweepy listener as and inherited class
#   We have to use as arguments:
#       - consumerKey: Consumer key provided by Twitter Dev API
#       - consumerSecret: Consumer secret provided by Twitter Dev API
#       - accessToken: Access token provided by Twitter Dev API
#       - accessTokenSecret: Access token secret provided by Twitter Dev API
#       - output_file: File path of a blank .txt file. This one will allocate all tweets/retweets/quoted_tweets text fetched.
#                       !!!! This output file needs to be created manually in your directory
#
#   Dependecies required: tweepy json pandas sys   
#
#   About tweepy library:
#       - This script was tested using version 4.4.0
#       - For this case tweepy no longer require StreamListener class to stream tweets. That's why we are using Stream class
# -------------------------------------------------------------------------------------------------------------------------------


# Class with Stream (from tweepy) object inherited
class Listener(tweepy.Stream):

    # Initiliaze contructor with required arguments and initial variables
    def __init__(self, consumerKey, consumerSecret, accessToken, accessTokenSecret, output_file):

        # Inherit all from Stream instance using 'super' function a temporary object that allows
        # reference to a parent class (tweepy.Stream)
        super(Listener,self).__init__(consumerKey, consumerSecret, accessToken, accessTokenSecret)

        # Read TXT output File Path
        self.TextOut = open(output_file,"w", encoding="utf-8")

        # Lists to allocate tweets, retweets and quoted tweets
        self.tweets = []
        self.retweets = []
        self.quoted = []
        
        # Desired keys to extract from tweet
        self.keys4tweets = ['created_at','id','text','favorite_count','retweet_count','lang','geo','coordinates','source','user','entities']

        # Columns for subseting final output
        self.sub_columns = ['created_at','id','text','favorite_count','retweet_count','lang','geo','coordinates','source','user.name',
                        'user.screen_name','user.location','entities.hashtags','entities.user_mentions']

        # Counter Number of tweets fetched
        self.status_counter = 0


    # Function to transform a 'tweepy.models.Status' object into a string and then into a json string
    def jsonify_tweepy(self, tweepy_object):
        # Write : Transform the tweepy's object as a json 
        json_str = json.dumps(tweepy_object._json, indent = 2)
        # Read : Transform the json into a Python Dictionary
        return json.loads(json_str)


    def on_status(self, status): # Once a new tweet is detected
        # Load initial objects
        TextOut = self.TextOut
        tweets = self.tweets
        retweets = self.retweets
        quoted = self.quoted
        keys4tweets = self.keys4tweets

        # Increasing counter of status fetched
        self.status_counter = self.status_counter + 1

        # Call method to transform json objects into dictionaries
        jsonify_tweepy = self.jsonify_tweepy

        #-----------------------------------------------------------------------------------------------------
        # VALIDATOR for kind of status on process
        #-----------------------------------------------------------------------------------------------------

        # FOR RETWEETS
        if hasattr(status, "retweeted_status"): #Check if retweet
            try:
                # Reorganize text for dictionary and text file
                text = str(status.retweeted_status.extended_tweet["full_text"])
                text = text.replace('\n',' ').replace('\r', '')
                # Subset of status object
                status = jsonify_tweepy(status)
                subset = {key: status['retweeted_status'][key] for key in keys4tweets}
                # Saving tweet in list
                retweets.append(subset)

                # Saving text
                TextOut.write(text)
                TextOut.write('\n')

            except AttributeError:
                # Reorganize text for dictionary and text file
                text = str(status.retweeted_status.text)
                text = text.replace('\n',' ').replace('\r', '')
                # Subset of status object
                status = jsonify_tweepy(status)
                subset = {key: status['retweeted_status'][key] for key in keys4tweets}
                # Saving tweet in list
                retweets.append(subset)

                # Saving text
                TextOut.write(text)
                TextOut.write('\n')


        # FOR QUOTED TWEETS
        elif hasattr(status, "quoted_status"): #Check if quoted tweet
            try:
                # Reorganize text for dictionary and text file
                text = str(status.quoted_status.extended_tweet["full_text"])
                text = text.replace('\n',' ').replace('\r', '')
                # Subset of status object
                status = jsonify_tweepy(status)
                subset = {key: status['quoted_status'][key] for key in keys4tweets}
                # Saving tweet in list
                quoted.append(subset)

                # Saving text
                TextOut.write(text)
                TextOut.write('\n')

            except AttributeError:
                # Reorganize text for dictionary and text file
                text = str(status.quoted_status.text)
                text = text.replace('\n',' ').replace('\r', '')
                # Subset of status object
                status = jsonify_tweepy(status)
                subset = {key: status['quoted_status'][key] for key in keys4tweets}
                # Saving tweet in list
                tweets.append(subset)

                # Saving text
                TextOut.write(text)
                TextOut.write('\n')


        # FOR REGULAR TWEETS
        else: # If it is a tweet
            try:
                # Reorganize text for dictionary and text file
                text = str(status.extended_tweet["full_text"])
                text = text.replace('\n',' ').replace('\r', '')
                # Subset of status object
                status = jsonify_tweepy(status)
                subset = {key: status[key] for key in keys4tweets}
                # Saving tweet in list
                tweets.append(subset)

                # Saving text
                TextOut.write(text)
                TextOut.write('\n')

            except AttributeError:
                 # Reorganize text for dictionary and text file
                text = str(status.text)
                text = text.replace('\n',' ').replace('\r', '')
                # Subset of status object
                status = jsonify_tweepy(status)
                subset = {key: status[key] for key in keys4tweets}
                # Saving tweet in list
                tweets.append(subset)

                # Saving text
                TextOut.write(text)
                TextOut.write('\n')


    # IN CASE WE REACH THE LIMIT (credit: csmath YouTube Channel)
    def on_error(self, status_code): # In case we exceed the limit
        if status_code == 420:
            print('You have been rate-limited for making too many requests.')
            return False



# -------------------------------------------------------------------------------------------------------------------------------
# Wrangler class 
# -------------------------------------------------------------------------------------------------------------------------------
#   This class is used to concatenate all dataframes tweets, retweets and quoted tweets into a single DataFrame
#   Then fix datatype formats
#
#   Dependecies required: pandas  
# -------------------------------------------------------------------------------------------------------------------------------

class Wrangler:
    def __init__(self, tweets_list, retweets_list, quoted_list, columns):
        # Generate required Dataframes once finished
        tweetsDF = pd.json_normalize(tweets_list)[columns]
        retweetsDF = pd.json_normalize(retweets_list)[columns]
        quotedDF = pd.json_normalize(quoted_list)[columns]

        # Generate an identifier for the kind of tweet
        tweetsDF['kind'] = ['Tweet' for i in range(tweetsDF.shape[0])]
        retweetsDF['kind'] = ['Retweet' for i in range(retweetsDF.shape[0])]
        quotedDF['kind'] = ['Quoted' for i in range(quotedDF.shape[0])]

        # Append all DF into a single one
        tweets = pd.concat([tweetsDF, retweetsDF, quotedDF], ignore_index=True)

        # Fix date format
        tweets['created_at'] = pd.to_datetime(tweets['created_at'])

        # Save Dataframe
        self.tweets = tweets



# -------------------------------------------------------------------------------------------------------------------------------
# About script
# -------------------------------------------------------------------------------------------------------------------------------
#   Developed by @robguilarr on January 2022, tested with average fetch-time of 30 tweets per second