from pymongo import MongoClient
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from datetime import datetime
import json
import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer


# <----------- Este trecho de código serve especificamente para a Autenticação necessária para a coneção ----------->
# Criando as Consumer's key
consumer_key = 'XXXXXXX'
consumer_secret = 'XXXXXXX'
# Criando os token's
access_token = 'XXXXXXX'
access_token_secret = 'XXXXXXX'

auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
# <----------- END ----------->

class MyListener(StreamListener):
    """A classe herda StreamListener. """
    def on_data(self, dados):
        """Converte os dados recebidos para Json. As variáveis capturam partes do Tweet (Data de criação, ID e o Texto) e no final, monta /
        um objeto com elas, insere no MongoDB e, por fim, printa na tela."""
        tweet = json.loads(dados)
        created_at = tweet["created_at"]
        id_str = tweet["id_str"]
        text = tweet["text"]
        obj = {"created_at":created_at,"id_str":id_str,"text":text,}
        tweetind = col.insert_one(obj).inserted_id
        print (obj)
        return True

# Faz a conexão com o MongoDB.
client = MongoClient('localhost', 27017)
# Cria um banco de dados 'twitterdb'
db = client.twitterdb
# Cria uma Collection 'tweets' no banco de dados 'twitterdb'
col = db.tweets
# Cria uma lista de palavras-chaves
keywords = ['Big Data', 'Python', 'Data Mining', 'Data Science']

myListener = MyListener()

# Passa a autenticação e o objeto myListener, cujo vai 'escutar' os tweets
# myStream = Stream(auth, listener = myListener)

# Filtra os tweets para receber somente tweets cujo tema seja um que esteja dentro da lista keywords
# myStream.filter(track=keywords)

# Fecha a conexão da Stream com o Twitter.
# myStream.disconnect()

# print(col.find_one())

# criando um dataset com dados retornados do MongoDB
dataset = [{"created_at": item["created_at"], "text": item["text"],} for item in col.find()]

df = pd.DataFrame(dataset)
print(df)

# Usando o método CountVectorizer para criar uma matriz de documentos
# cv = CountVectorizer()
# count_matrix = cv.fit_transform(df.text)

# Contando o número de ocorrências das principais palavras no dataset
# word_count = pd.DataFrame(cv.get_feature_names(), columns=["word"])
# word_count["count"] = count_matrix.sum(axis=0).tolist()[0]
# word_count = word_count.sort_values("count", ascending=False).reset_index(drop=True)
# print(word_count[:50])