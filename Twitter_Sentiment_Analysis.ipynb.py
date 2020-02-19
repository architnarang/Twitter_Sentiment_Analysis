#!/usr/bin/env python
# coding: utf-8

# In[1]:


import tweepy
import jsonpickle
import csv
import time
st = time.time()
API_KEY="z5fwBpqXKReGZdR9qI2VyYsvs"
API_SECRET="5GaSPBTvEyKvv4G2rku1d13Lw05iz6MbnwKCprfjatYClkmsBq"
ACCESS_TOKEN="1479289146-kvsxXbGz5mWKmunsKpeoeJwOqwZ7VZUdeNXggx4"
ACCESS_TOKEN_SECRET="kAk1y1Jk75FFU8jnILW57qXoiRT4bTTFikbTYEZBMDdob"
auth = tweepy.OAuthHandler(API_KEY, API_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
api = tweepy.API(auth,wait_on_rate_limit=True,wait_on_rate_limit_notify=True)
print(api.me().name)


# In[2]:


tweetsPerQuery = 100#this is the maximum provided by API
max_tweets = 100 # just for the sake of While loop
fName = 'sharomo.txt' # where i save the tweets


# In[3]:


since_id = None
max_id = -1
tweet_count = 0
print("Downloading the tweeets..takes some time..")

search_query="\"#SwachhBharat\""


# In[4]:


search_query="\"#SwachhBharat\""
x=0
with open(fName,'w') as f:
    print("Downloading hashtag" + search_query)
    
    while(tweet_count<max_tweets):
        try:
            if(max_id<=0):
                if(not since_id):
                    new_tweets = api.search(q=search_query,count=tweetsPerQuery,lang="en",tweet_mode='extended')
                else:
                    new_tweets = api.search(q=search_query,count=tweetsPerQuery,lang="en",tweet_mode='extended',since_id=since_id)
            
            else:
                if(not since_id):
                    new_tweets = api.search(q=search_query,count=tweetsPerQuery,lang="en",tweet_mode='extended',max_id=str(max_id-1))
                else:
                    new_tweets = api.search(q=search_query,count=tweetsPerQuery,lang="en",tweet_mode='extended',max_id=str(max_id-1),since_id=since_id)
            
            # Tweets Exhausted
            if(not new_tweets):
                print("No more tweets found!!")
                break
            # write all the new_tweets to a json file
            for tweet in new_tweets:
                f.write(jsonpickle.encode(tweet._json,unpicklable=False)+'\n')
                tweet_count+=1
                print("Successfully downloaded {0} tweets".format(tweet_count))
                max_id=new_tweets[-1].id
        # in case of any error
        except tweepy.TweepError as e:
            print("Some error!!:"+str(e))
            break
end = time.time()
print("A total of {0} tweets are downloaded and saved to {1}".format(tweet_count,fName))
print("Total time taken is ",end-st,"seconds.")


# In[5]:


import json
import csv
f = open('som2.csv','a',encoding='utf-8')
csvWriter = csv.writer(f)
headers=['full_text','retweet_count','user_followers_count','favorite_count','place','coordinates','geo','created_at','id_str']
csvWriter.writerow(headers)
for inputFile in ['sharomo.txt']:#all the text-file names you want to convert to Csv in the sae folder as this code
    tweets = []
    for line in open(inputFile, 'r'):
        tweets.append(json.loads(line))

    print('HI',len(tweets))
    
    count_lines=0

    for tweet in tweets:
        try:
            csvWriter.writerow([tweet['full_text'],tweet['retweet_count'],tweet['user']['followers_count'],tweet['favorite_count'],tweet['place'],tweet['coordinates'],tweet['geo'],tweet['created_at'],str(tweet['id_str'])])
            count_lines+=1
        except Exception as e:
            print(e)
    print(count_lines)


# In[6]:


import pandas as pd
df = pd.read_csv('som2.csv', encoding = 'unicode_escape')
df.info()


# In[7]:


df


# In[8]:


print(len(df.index))#14195
serlis=df.duplicated().tolist()
print(serlis.count(True))#112
serlis=df.duplicated(['full_text']).tolist()
print(serlis.count(True))#8585


# In[9]:


df=df.drop_duplicates(['full_text'])
df.head()


# In[10]:


df=df.drop(["place","coordinates","geo","id_str"],axis=1)
df


# In[71]:


import csv, json
csvfile = open('som2.csv', 'r')
jsonfile = open('file.json', 'w')

fieldnames = ("FirstName","LastName","IDNumber","Message")
reader = csv.DictReader( csvfile, fieldnames)
for row in reader:
    json.dump(row, jsonfile)
    jsonfile.write('\n')


# In[3]:



import string
import re
import csv


from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize

class TweetCleaner:
    def __init__(self, remove_stop_words=False, remove_retweets=False, stopwords_file='NLTK_DEFAULT'):
        """
        clean unnecessary twitter data
        remove_stop_words = True if stopwords are to be removed (default = False)
        remove_retweets = True if retweets are to be removed (default = False)
        stopwords_file = file containing stopwords(one on each line) (default: nltk english stopwords)
        """
        if remove_stop_words:
            if stopwords_file == 'NLTK_DEFAULT':
                self.stop_words = set(stopwords.words('english'))
            else:
                stop_words = set()
                with open(stopwords_file,'r') as f:
                    for line in f:
                        line = line.replace('\n','')
                        stop_words.add(line.lower())
                        self.stop_words = stop_words
        else:
            self.stop_words = set()
        
        self.remove_retweets = remove_retweets
        
        self.punc_table = str.maketrans("", "", string.punctuation) # to remove punctuation from each word in tokenize


# In[4]:



   def compound_word_split(self, compound_word):
       """
       Split a given compound word(string) and return list of words in given compound_word
       Ex: compound_word='pyTWEETCleaner' --> ['py', 'TWEET', 'Cleaner']
       """
       matches = re.finditer('.+?(?:(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])|$)', compound_word)
       return [m.group(0) for m in matches]


# In[5]:



def remove_non_ascii_chars(self, text):
        """
        return text after removing non-ascii characters i.e. characters with ascii value >= 128
        """
        return ''.join([w if ord(w) < 128 else ' ' for w in text])


# In[6]:



def remove_hyperlinks(self,text):
        """
        return text after removing hyperlinks
        """
        return ' '.join([w for w in text.split(' ')  if not 'http' in w])


# In[7]:



def get_cleaned_text(self, text):
        """
        return cleaned text(string) for provided tweet text(string)
        """
        cleaned_text = text.replace('\"','').replace('\'','').replace('-',' ')
        cleaned_text =  self.remove_non_ascii_chars(cleaned_text)
        
        # retweet
        if re.match(r'RT @[_A-Za-z0-9]+:',cleaned_text): # retweet
            if self.remove_retweets: return ''
            retweet_info = cleaned_text[:cleaned_text.index(':')+2] # 'RT @name: ' will be again added in the text after cleaning
            cleaned_text = cleaned_text[cleaned_text.index(':')+2:]
        else:
            retweet_info = ''
        cleaned_text = self.remove_hyperlinks(cleaned_text)
        cleaned_text = cleaned_text.replace('#','HASHTAGSYMBOL').replace('@','ATSYMBOL') # to avoid being removed while removing punctuations
        
        tokens = [w.translate(self.punc_table) for w in word_tokenize(cleaned_text)] # remove punctuations and tokenize
        tokens = [w for w in tokens if not w.lower() in self.stop_words and len(w)>1] # remove stopwords and single length words
        cleaned_text = ' '.join(tokens)
        
        cleaned_text = cleaned_text.replace('HASHTAGSYMBOL','#').replace('ATSYMBOL','@')
        cleaned_text = retweet_info + cleaned_text
        
        return cleaned_text
        


# In[8]:



def get_cleaned_tweet(self, tweet):
        """
        return a json dictionary of cleaned data from provided original tweet json dictionary
        """
        if not "created_at" in tweet: return None # remove info about deleted tweets
        if not tweet['lang'] == 'en': return None # remove tweets in non english language
        if not tweet['in_reply_to_status_id'] == None or not tweet['in_reply_to_user_id'] == None: return None # remove comments of any tweet
        
        cleaned_text = self.get_cleaned_text(tweet['text'])
        if cleaned_text == '': return None

        cleaned_tweet = {}
        
        cleaned_tweet['created_at'] = tweet['created_at']
        cleaned_tweet['full_text'] = cleaned_text
        
        cleaned_tweet['user'] = {}
        cleaned_tweet['user']['favourite_count'] = tweet['user']['favourite_count']
         
        
        cleaned_tweet['retweet_count'] = tweet['retweet_count']

        return cleaned_tweet


# In[13]:



def clean_tweets(self, input_file, output_file='cleaned_tweets.json'):    
        """
        input_file: name or path of input twitter json data where each line is a json tweet
        output_file: file name or path where cleaned twitter json data is stored (default='cleaned_tweets.json')
        """
        in_file = open(input_file, 'r')
        out_file = open(output_file, 'w')
        
        while True:
            line = in_file.readline()
            if line=='': break
            tweet = json.loads(line)
            
            cleaned_tweet = self.get_cleaned_tweet(tweet)
            if cleaned_tweet == None: continue
            """
            if 'retweeted_status' in tweet: # will be present if it is a retweet
                cleaned_tweet['retweeted_status'] = self.get_cleaned_tweet(tweet['retweeted_status'])
                if cleaned_tweet['retweeted_status'] == None: continue
            """
                
            out_file.write(json.dumps(cleaned_tweet)+'\n')
            
        
        in_file.close()
        out_file.close()
    


# In[17]:



if __name__  == '__main__':
    
    tc = TweetCleaner(remove_stop_words=False, remove_retweets=False)
    clean_tweets(input_file='file.json', output_file='cleaned_tweets.json') # clean tweets from entire file
    print('Output with remove_stop_words=False, remove_retweets=False:')
    print(tc.get_cleaned_text(sample_text), '\n')
    
    tc = TweetCleaner(remove_stop_words=False, remove_retweets=True)
    print('Output with remove_stop_words=False, remove_retweets=True:')
    print(tc.get_cleaned_text(sample_text), '\n')
    
    tc = TweetCleaner(remove_stop_words=True, remove_retweets=False, stopwords_file='user_stopwords.txt')
    print('Output with remove_stop_words=True, remove_retweets=False:')
    print(tc.get_cleaned_text(sample_text))


# In[80]:


import re
for i in range(len(df)):
    txt = df.loc[i]["full_text"]
    txt=re.sub(r'@[A-Z0-9a-z_:]+','',txt)#replace username-tags
    txt=re.sub(r'^[RT]+','',txt)#replace RT-tags
    txt = re.sub('https?://[A-Za-z0-9./]+','',txt)#replace URLs
    txt=re.sub("[^a-zA-Z]", " ",txt)#replace hashtags
    df.at[i,"full_text"]=txt


# In[ ]:




