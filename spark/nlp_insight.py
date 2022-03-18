from pyspark.sql import SparkSession
from pyspark.sql.functions import udf

from textblob import TextBlob
from textblob.sentiments import NaiveBayesAnalyzer

import nltk


def libnltk():
    nltk.download('movie_reviews')
    nltk.download('punkt')
    nltk.download('twitter_samples')


# Analisando os textos e retornando nota
def parsing_text(text):
    temp = TextBlob(text).sentiment[0]
    return temp


def apply_fuction(sentiment, df):
    df_sentiment = df.withColumn("sentiment", sentiment(df['text']))
    return df_sentiment
# Armazenar os text em uma variavel


def export_json(df_sentiment):
    df_sentiment.coalesce(1)\
        .write\
        .json("/run/datapipeline/datalake/gold/twitter_insight_tweet_sentiment")


def run(spark):
    df = spark.read.json(
        "/run/datapipeline/datalake/silver/twitter_bbb22/tweet")
    sentiment = udf(parsing_text)
    df_sentiment = apply_fuction(sentiment, df)
    export_json(df_sentiment)


if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("twitter_insight")\
        .getOrCreate()

    run(spark)
