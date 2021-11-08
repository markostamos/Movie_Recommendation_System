from elasticsearch import Elasticsearch,helpers
import pandas as pd
import os,sys,csv

#ανεβάζω τα δεδομένα ratings.csv στην elastic search
def load_data(csv_name,index_name):

  curdir = os.path.dirname(__file__)
  csv_file = curdir+"/datasets/"+csv_name
  f = open(csv_file,"r")
  dictionar = csv.DictReader(f)
  res = helpers.bulk(es,dictionar,index="ratings")
  print("Data Loaded")    

#επιστρέφει τις 10 πιο συναφείς ταινίες απο την elastic search
def get_movies(phrase,num = 10):
  query_body = {
    "query":{
      "bool":{
        "should":[{
          "match":{
            "title":{
              "query":phrase,
              "fuzziness": "AUTO"
              }
          }},
          {
          "match":{
            "genres":{
              "query":phrase,
              "fuzziness": "AUTO"
              }
          }}
        ]
      }
    }
  }
  
  res = es.search(index="movies",body=query_body,size=num)
  return res  

#ψάχνει στην elastic search για τη βαθμολογία του χρήστη με userID στην ταινία με movieId
#αν δεν εχει βαθμολογήσει ο χρήστης την ταινία επιστρέφει -1
def getUserEval(userId,movieId):
    query_body={
  "query": {
    "bool": {
      "must": [
        {
          "match": {
            "userId": userId
          }
        },{
        "match" : {
          "movieId":movieId
        }
        }
      ]
    }
  }
}
    res = es.search(index="ratings",body=query_body,filter_path=["hits.hits._source.rating"])
    if res:
        return float(res["hits"]["hits"][0]["_source"]["rating"])
    else: return -1

#ψάχνει τα ratings για ολες τις βαθμολογιες μιας ταινίας με movieId και επιστρέφει
#τον μέσο όρο των βαθμολογιών
def getAverageEval(movieId):
    query_body={
  "query": {
    "bool": {
      "must": [
      {
        "match" : {
          "movieId":movieId
        }
        }
      ]
    }
  }
}
    res= es.search(index="ratings",body=query_body,size=671)
    
    n = len(res["hits"]["hits"])
    if n==0: return 2.5
    rating = 0
    for x in res["hits"]["hits"]:
        rating = rating + float(x["_source"]["rating"])
    return rating/n    

def print_movies(res):
    if not res:
        print("no results")
        return
    else:
        for x in res:
            print()
            print("===========================")
            print("ID: ",str(x["_id"]))
            print("Title: ",x["_source"]["title"])
            print("Genres: ",x["_source"]["genres"])
            print('Normalized Elastic Rating: %.2f' % x["_oldScore"])
            print("Users Rating: ",x["_userEval"])
            print('Average Rating: %.2f / 5' % x["_movieEval"])
            print('New_Score: %.2f'% x["_score"],"/ 10")
            
##αν ο User δεν εχει βαθμολογησει μια ταινια 
#τοτε το βαρος μοιραζεται εξισου στο recall της elastic
#και στον μεσο ορο των βαθμολογιων
#πρακτικα το recall σπρωχνει την μετρικη κοντα στο 6 για ταινιες
#που ταιριαζουν με το query και ο μεσος ορος προστιθεται επιπλέον
#οταν ο user εχει βαθμολογήσει μια ταινία τότε μετράμε και αυτό
#για το αποτέλεσμα της μετρικής
#αν ο χρ΄ηστης έχει βαθμολογήσει με βαθμο >2.5 δηλαδη >0.5 (κανονικοποιημένο)
#τοτε προσμετριέται θετικά στο score της μετρικής
#αν ο χρήστης έχει βαθμολογήσει <2.5 σημαίνει ότι δεν του άρεσε η ταινία
#οπότε αφαιρείται απο το score.
#στην περίπτωση που ο χρήστης βαθμολογήσει με 2.5 
#δεν υπάρχει κάποια επίδραση στην μετρική.
def calculate_score(old_score,userEval,movieEval):
    if userEval>=0:
        return 6*old_score + 2*movieEval + 2*2*(userEval-0.5)
    else:
        return 6*old_score + 4*movieEval

#Επιστρέφει τις πιο συναφείς ταινίες σύμφωνα με την custom μετρική
def search_movie(phrase,userId,num=10):
    res = get_movies(phrase,num)

    if res["hits"]["total"]["value"]==0: return
    elastic_max_score = res["hits"]["max_score"]
    for x in res["hits"]["hits"]:
        userEval = getUserEval(movieId=x["_id"],userId=userId)
        movieEval = getAverageEval(x["_id"])
        x["_movieEval"]= movieEval
        old_score = x["_score"]/elastic_max_score
        x["_oldScore"] =  old_score
        if userEval>=0: x["_userEval"] = userEval
        else: x["_userEval"] = "N/A"
        new_score = calculate_score(old_score,userEval/5,movieEval/5)
        x["_score"] = new_score
        

    sorted_movies = sorted(res["hits"]["hits"], key=lambda x: x["_score"],reverse=True)
   
    return sorted_movies

        


if __name__=="__main__":
    es = Elasticsearch(host="localhost",port=9200,timeout=500)
    load_data("ratings.csv","ratings")
    a = input("Search for a movie: ")
    b = int(input("User ID: "))
    res = search_movie(phrase=a,userId=b,num=10)
    print_movies(res)
