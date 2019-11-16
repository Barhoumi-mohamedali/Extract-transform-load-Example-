from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import desc # Permettez-nous de commander par ordre décroissant
sc=SparkContext(appName="TweetApp6") # Ne peut executer q'une seul fois. redémarrez votre noyau pour détecter les erreurs .
ssc = StreamingContext(sc, 10 )
sqlContext = SQLContext(sc) # Permettez-nous d'utiliser SQL pour qeury

ssc.checkpoint( "file:///home/dataera/Bureau/tweet2")
socket_stream = ssc.socketTextStream("127.0.0.1", 5532)
lines = socket_stream.window( 20 )
from collections import namedtuple
fields = ("tag", "count" )

Tweet = namedtuple( 'Tweet', fields ) # Chaque élément se verra attribuer un champ
# Utilisez des parenthèses pour plusieurs lignes ou utilisez \.
( lines.flatMap( lambda text: text.split( " " ) ) #Divisé à une liste
  .filter( lambda word: word.lower().startswith("#") ) # Vérifie les hashtag
  .map( lambda word: ( word.lower(), 1 ) ) # Minuscules le mot, met en place un tuple
  .reduceByKey( lambda a, b: a + b )  # Reduces by key
  .map( lambda rec: Tweet( rec[0], rec[1] ) ) # Mettre dans un objet Tweet
  .foreachRDD( lambda rdd: rdd.toDF().sort( desc("count") ) # Trie par ordre décroissant par nombre
              .limit(10).registerTempTable("tweets") ) ) # Pour chaque dix tweets sera enregistré comme une table.

sqlContext
ssc.start()

import matplotlib.pyplot as plt #Bibliothèque de visualisation
import seaborn as sn #Bibliothèque de visualisation
get_ipython().magic(u'matplotlib inline')



import time 
top_100_tweets = sqlContext.sql( "Select tag, count from tweets" )
print(top_100_tweets.show(100))

from IPython import display #Nous permet d'afficher des éléments dans le notebook
count = 0
while count < 100:
    time.sleep( 20 )
    top_100_tweets = sqlContext.sql( "Select tag, count from tweets" )
    top_100_df = top_100_tweets.toPandas() # Bibliothèque de Dataframe
    display.clear_output(wait=True) #Efface la sortie, si un plot existe.
    plt.figure( figsize = ( 10, 8) )
    sn.barplot( x="count", y="tag", data=top_100_df)
    plt.show()
    count = count + 1

ssc.stop()
