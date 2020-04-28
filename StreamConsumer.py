from kafka import KafkaConsumer
import json
from elasticsearch import Elasticsearch
from textblob import TextBlob
es = Elasticsearch()
import re

def main():
    '''
    main function initiates a kafka consumer, initialize the tweetdata database.
    Consumer consumes tweets from producer extracts features, cleanses the tweet text,
    calculates sentiments and loads the data into postgres database
    '''
    # set-up a Kafka consumer
    consumer = KafkaConsumer("twitter")
    for msg in consumer:
        dict_data = json.loads(msg.value)
        sentiment='neutral'
        if dict_data['lang']=='en':
            print(type(dict_data))
            print(dict_data['text'])
            preprocess = clean_tweet(dict_data["text"])
            # print(preprocess)
            tweet = TextBlob(preprocess)
            print(tweet)
            testimonial =tweet.sentiment.polarity
            print(testimonial)
            if testimonial>0:
                sentiment='positive'
            elif testimonial<0:
                sentiment='negative'
            # add text and sentiment info to elasticsearch
            es.index(index="coron_test",
                      doc_type="test-type",
                        body={"author": dict_data["user"]["screen_name"],
                            "date": dict_data["created_at"],
                              "message": sentiment,
                              })
            print('\n')

def clean_tweet(tweet):
        '''
        Utility function to clean tweet text by removing links, special characters
        using simple regex statements.
        '''
        # x=re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t]) | (\w+:\ / \ / \S+)", " ", tweet)
        # without_aphostrophe = re.sub("'s", "", imp_content)
        # without_hyphen = re.sub("[.\-]", "", without_aphostrophe)
        x=re.sub("[@,\/#!$%\^&\*;:{}=_`~()+']", " ", tweet)
        print(x)
        # without_hyphen_punct_num = re.sub("[0-9]+", "", without_hyphen_punct)
        return " ".join(x.split());


if __name__ == "__main__":
    main()