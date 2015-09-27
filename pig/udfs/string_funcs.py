from pig_util import outputSchema

@outputSchema('word:chararray')
def reverse(word):
   """
   Return the reverse text of the provided word
   """
   return word[::-1]


@outputSchema('length:int')
def num_chars(word):
   """
   Return the length of the provided word
   """
   return len(word)