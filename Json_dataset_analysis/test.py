# import urllib library
from urllib.request import urlopen
  
# import json
import json
# store the URL in url as 
# parameter for urlopen
url = "https://rest.uniprot.org/uniprotkb/search?format=fasta&query=%28RNA%20binding%29&size=500"
  
# store the response of URL
response = urlopen(url)
  
# storing the JSON response 
# from url in data
data_json = json.loads(response.read())
  
# print the json response
print(data_json)