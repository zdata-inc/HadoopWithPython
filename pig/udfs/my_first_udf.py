from pig_util import outputSchema

@outputSchema('value:int')
def return_one():
   """
   Return the integer value 1
   """
   return 1