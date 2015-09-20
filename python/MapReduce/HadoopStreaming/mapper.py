#!/usr/bin/python
import sys

for line in sys.stdin:
   words = line.split()

   for word in words:
      print '{0}\t{1}'.format(word, 1)