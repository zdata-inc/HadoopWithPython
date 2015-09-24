from snakebite.client import Client

client = Client('localhost', 9000)
for l in client.text(['/input/input.txt']):
   print l