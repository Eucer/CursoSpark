
import re
from pyspark import SparkConf, SparkContext
import collections


def normalizeWords(text):
    return re.compile(r'\w+', re.UNICODE).split(text.lower())


conf = SparkConf().setMaster("local").setAppName("ContarPalabras")
sc = SparkContext(conf=conf)

input = sc.textFile("file:///CursoSpark/libro.txt")
words = input.flatMap(normalizeWords)
wordCount = words.countByValue()


for word, count in wordCount.items():
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))
