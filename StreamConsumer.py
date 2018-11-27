from kafka import KafkaConsumer
from textblob import TextBlob
from nltk.sentiment.vader import SentimentIntensityAnalyzer
#from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import json



def main():
    f = open("out4.txt", "w+")
    consumer = KafkaConsumer('twitter')
    sid = SentimentIntensityAnalyzer()
    for msg in consumer:
        output = []
        p=""
        output.append(json.loads(msg.value))
        stringTweet = str(output[0]['text'].encode('utf-8'))
        strout=str(output)
        score = sid.polarity_scores(strout)['compound']
        score1= TextBlob(strout).sentiment.polarity
        if score >0:
            f.write(stringTweet+"\nPositive Sentiment\n\n")
        elif score<0:
            f.write(stringTweet + "\nNegative Sentiment\n\n")
        else:
            f.write(stringTweet + "\n Neutral Sentiment\n\n")
        f.flush()
        print('\n')





if __name__ == "__main__":
    main()
