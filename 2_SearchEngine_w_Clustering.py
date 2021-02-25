from elasticsearch import Elasticsearch,helpers
import pandas as pd
import csv
import os,sys,csv

#Σε αυτό το script ουσιαστικά λύνεται η άσκηση 3.
#Ο κώδικας είναι ολόιδιος με τον κώδικα της άσκησης 2 με την διαφορά ότι όταν ένας χρήστης
#δεν έχει δει κάποια ταινία τότε αντι για N/A κοιτάμε τον cluster στον οποίο ανήκει ο χρ΄ήστης
#και βρίσκουμε τον μέσο όρο των βαθμολογιών για αυτή την ταινία
#αν και πάλι δεν έχουμε τιμές, αναγράφεται το N/A

def load_data(csv_name,index_name):
  curdir = os.path.dirname(__file__)
  csv_file = curdir+"/datasets/"+csv_name
  
  f = open(csv_file,"r")
  dictionar = csv.DictReader(f)
  res = helpers.bulk(es,dictionar,index=index_name)
  print("Data Loaded") 

def load_csv(csv_name):
    curdir = os.path.dirname(__file__)
    csv_file = curdir+"./datasets/"+csv_name
    csv = pd.read_csv(csv_file)
    return csv
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

#φορτώνει το αρχείο clusters.csv και βρίσκει τον cluster στον οποίο ανήκει ο χρήστης
#και στη συνέχεια υπολογίζει τον μέσο όρο των βαθμολογιών του cluster για την συγκεκριμένη
#ταινια.
#Αν κανένας απο τον cluster δεν έχει δει την ταινία επιστρέφει τιμή -1
def getClusterEval(userId,movieId):
    clusters = load_csv("clusters.csv")
    userCluster = clusters[clusters["userId"]==userId].iloc[0]["cluster"]
    userList = clusters[clusters["cluster"]==userCluster]
    clusterAverage = {"val":0,"n":0}
    for index,row in userList.iterrows():
        query_body={
        "query": {
            "bool": {
                "must": [
                {
                    "match": {
                        "userId": int(row["userId"])
                    }
                },{
                    "match" : {
                        "movieId":int(movieId)
                    }
                }
                    ]
            }
        }
        }
        res = es.search(index="ratings",body=query_body,filter_path=["hits.hits._source.rating"])
        
        if res:
            clusterAverage["val"]+=float(res["hits"]["hits"][0]["_source"]["rating"])
            clusterAverage["n"]+=1
    if clusterAverage["n"]!=0:
        return clusterAverage["val"]/clusterAverage["n"]
    else:
        return -1             

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
    if n==0: return 0
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
            
            if x["_cluster"]:
                print('Users Rating: N/A')
                print('User Cluster Rating: ',x["_userEval"])
            else:
                print('Users Rating: ',x["_userEval"])    
            print('Average Rating: %.2f / 5' % x["_movieEval"])
            print('New_Score: %.2f'% x["_score"],"/ 10")
            
#η Calculate score δεν αλλάζει, αν ένας χρήστης δεν έχει δει μια ταινία
#αλλα  υπάρχει βαθμολογία απο τον cluster του τότε χρησιμοποιείται αυτή για userEval.
def calculate_score(old_score,userEval,movieEval):
    if userEval>=0:
        return 6*old_score + 2*movieEval + 2*2*(userEval-0.5)
    else:
        return 6*old_score + 4*movieEval


#To μόνο που αλλάζει εδώ είναι οτι αν δεν υπάρχει βαθμολογία του χρήστη για την ταινία
#καλείται η συνάρτηση getClusterEval για να βρεθεί ο μέσος όρος των βαθμολογιών του cluster.
def search_movie(phrase,userId,num=10):
    res = get_movies(phrase,num)
    if res["hits"]["total"]["value"]==0: return
    elastic_max_score = res["hits"]["max_score"]
    for x in res["hits"]["hits"]:
        userEval = getUserEval(movieId=x["_id"],userId=userId)
        ##αν το userEval=-1 δες τον cluster του user
        if userEval==-1:
            userEval = getClusterEval(userId=userId,movieId=x["_id"])
            x["_cluster"] = True 
        movieEval = getAverageEval(x["_id"])
        x["_movieEval"]= round(movieEval,1)
        old_score = x["_score"]/elastic_max_score
        x["_oldScore"] =  old_score
        if userEval>=0: x["_userEval"] = round(userEval,1)
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
