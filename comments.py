import os
import sys
import requests
from elasticsearch import Elasticsearch

LISTPOSTS_ENDPOINT = "https://disqus.com/api/3.0/threads/listPosts.json"

try:  
   api_key = os.environ["DISQUS_APP_KEY"]
except KeyError: 
   print("DISQUS_APP_KEY requered")
   sys.exit(1)

def upload_comments(host, port, index_name, forum, thread):
    ''' 
    Gets comments from a specific url using Disqus API and inserst them to Elasticsearch index
    '''
    es = Elasticsearch(host=host, port=port)

    payload = {"api_key": api_key, "forum": forum, "thread": thread}

    while True:
        response = requests.get(LISTPOSTS_ENDPOINT, params=payload)

        if response.status_code == 200:
            response_data = response.json()

            messages = response_data["response"]       

            for item in messages:
                print("id: {} message: {}".format(item["id"], item["raw_message"]))
                es.create(index=index_name, id=item["id"], body={"comment" : item["raw_message"], "origin": thread})
            
            if response_data["cursor"]["hasNext"]:
                payload = {"api_key": api_key, "forum": forum, "thread": thread, "cursor": response_data["cursor"]["next"]}
            else:
                break
        else:
            print(response.json()["response"])
            break


if __name__ == "__main__":
    if len(sys.argv) < 6:
        print("Missing arguments: ['host' 'port' 'index' 'forum' 'url']")
        sys.exit(1) 
    link = sys.argv[5]
    thread = "ident:{}".format(link)
    upload_comments(host=sys.argv[1], port=sys.argv[2], index_name=sys.argv[3] , forum=sys.argv[4], thread=thread) 
