from pyspark import SparkContext
from operator import add
import sys

APP_NAME = 'SparkWordCount'

def main():

   if len(sys.argv) != 3:
      sys.stderr.write('Usage: {} <input_file> <output_file>'.format(sys.argv[0]))
      sys.exit()

   sc = SparkContext(appName=APP_NAME)

   lines = sc.textFile(sys.argv[1])
   words = lines.flatMap(lambda line: line.split())
   counts = words.map(lambda word: (word, 1))
   frequency = counts.reduceByKey(add)

   frequency.saveAsTextFile(sys.argv[2])

if __name__ == '__main__':
   main()