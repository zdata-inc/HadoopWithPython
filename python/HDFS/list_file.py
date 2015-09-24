from snakebite.client import Client

client = Client('localhost', 9000)
for x in client.ls(['/input.txt']):
   print x