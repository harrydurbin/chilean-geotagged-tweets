# chile-tweets

The project collects tweets from the twitter API that are geo-tagged and located within Chile. To accomplish this, the API stream is first filtered to a rectangle around the vicinity of Chile, and then the coordinates are converted to a GIS point to query whether the point intersects a Chile boundary shapefile.   

After collecting 10,000 tweets, a word cloud of hastags was created.


![alttag](https://github.com/harrydurbin/chilean-tweets/blob/master/img/chile_tweets.png)  


![alttag](https://github.com/harrydurbin/chilean-tweets/blob/master/img/chilewordcloud.png)

