from pig_util import outputSchema
from datetime import datetime
import re


@outputSchema('title:chararray')
def parse_title(title):
   """
   Return the title without the year
   """
   return re.sub(r'\s*\(\d{4}\)','', title)

@outputSchema('days_since_release:int')
def days_since_release(date):
   """
   Calculate the number of days since the titles release
   """
   if date is None:
      return None

   today = datetime.today()
   release_date = datetime.strptime(date, '%d-%b-%Y')
   delta = today - release_date
   return delta.days