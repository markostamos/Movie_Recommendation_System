from elasticsearch import Elasticsearch,helpers
import pandas as pd
import csv
import os,sys,csv
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
import keras
#Αυτή είναι η τελική μηχανή αναζήτησης
#παρόμοια με την τελευταία με την διαφορά ότι όταν δεν έχει βαθμολογήσει ένας χρήστης
#μια ταινία τότε για βαθμολογία θεωρείται ο μέσος όρος των βαθμολογιών
#που δίνεται απο την μέθοδο clustering και νευρωνικών
#Γενικά παρατηρήθηκε ότι οι τιμές αυτές είναι πολύ κοντά μεταξύ τους που σημαίνει ότι
#το νευρωνικό δίνει μεγαλύτερη έμφαση στα είδη των ταινιών και όχι στον τίτλο της ταινίας
#Αυτό ίσως γίνεται για δύο λόγους
# α) φταίει η κλίμακα των Inputs δηλαδή τα vectors των τίτλων έχουν μικ΄ρες τιμές σε σχέση
#       με τις τιμές των One hot encoding (0 η 1). το οποίο δεν γνωρίζουμε αν πρέπει να
#       το διορθώσουμε καθώς δεν έχουμε εμπιστοσύνη στο μοντέλο των word embeddings που
#       δημιουργήθηκε. 

#Αυτό γιατί:
# b) Το μοντέλο των word embeddings αν και "δουλεύει" δηλαδή μετατρέπει τιτλους σε διανύσματα
#    λόγο του μικρού πλήθους δεδομένων ίσως δεν έχει δημιουργήσει συσχετίσεις μεταξύ αυτών.
#    Με λίγα λόγια, κάθε τίτλος είναι ένα vector άρα μπορεί να χρησιμοποιηθεί στο νευρωνικό
#    αλλά λόγω μικρού πλήθους θα είναι ένα σχεδόν
#    τυχαίο vector επομένως δεν θα έχει κάποια στατιστική σημασία και θα λειτουργεί ώς θόρυβος
#    μικρού πλάτους που το νευρωνικό αποβάλλει μόνο του, γιαυτό και οι έξοδοι είναι πολύ
#    κοντά σε αυτές των clusters που χρησιμοποιούν μόνο τα είδη των ταινιών.
#
# Για να λυθεί αυτό το πρόβλημα θα μπορούσαμε να πάρουμε ένα μοντέλο word embeddings
# το οποίο να είναι ήδη εκπαιδευμένο στην αγγλική γλώσσα όπου εκεί αν εμφανιζόταν το ίδιο
# πρόβλημα θα υπήρχε λόγος να "παίξουμε" με τις κλίμακες ω΄στε να δώσουμε βάρος είτε
# στους τίτλους είτε στα είδη των ταινιών.
# 
# Επομένως η χρησιμότητα του νευρωνικού στη συγκεκριμένη μηχανή αναζήτησης ταινιών
# είναι ότι δίνει τιμές στις περιπτώσεις που ο cluster του χρήστη δεν έχει να δώσει
# βαθμολογία.
def load_data(csv_name,index_name):
  curdir = os.path.dirname(__file__)
  csv_file = curdir+"/datasets/"+csv_name
  f = open(csv_file,"r")
  dictionar = csv.DictReader(f)
  res = helpers.bulk(es,dictionar,index=index_name)
  print("Data Loaded")    
def load_csv(csv_name):
    csv = pd.read_csv("datasets/"+csv_name)
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
def getPrediction(movieId,model):
    encoded_movies = load_csv('encodedMovies.csv')
    movies = encoded_movies[encoded_movies["movieId"]==int(movieId)]
    movie = movies.drop(['movieId'],axis=1)
    evaluation = model.predict([movie])
    return round(evaluation[0][0],1)
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
            
            if x["_model"]:
                print('Users Rating: N/A')
                print('Cluster Average Rating: ',x["_clusterEval"])
                print('Neural Network Rating: ',x["_modelEval"])
                print('Average predicted Rating: ',x["_userEval"])
            else:
                print('Users Rating: ',x["_userEval"])    
            print('Average Rating: %.2f / 5' % x["_movieEval"])
            print('New_Score: %.2f'% x["_score"],"/ 10")
           

def calculate_score(old_score,userEval,movieEval):
    return 6*old_score + 2*movieEval + 2*2*(userEval-0.5)

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
def search_movie(phrase,userId,num=10):
    res = get_movies(phrase,num)
    if res["hits"]["total"]["value"]==0: return

    elastic_max_score = res["hits"]["max_score"]

    #load the user Model
    path = 'models/'+str(userId)
    user_model = keras.models.load_model(path)

    for x in res["hits"]["hits"]:
        userEval = getUserEval(movieId=x["_id"],userId=userId)
        ##αν το userEval= N/A δες τον cluster του user
        if userEval==-1:
            modelEval = getPrediction(movieId=x["_id"],model = user_model)
            clusterEval = getClusterEval(userId,x["_id"])
            x["_modelEval"]=round(modelEval,1)
            x["_clusterEval"]=round(clusterEval,1)
            if clusterEval!=-1:
                userEval = (modelEval+clusterEval)/2
                userEval = round(userEval,1)
            else:
                userEval = modelEval  
                x["_clusterEval"]="N/A"  
            x["_model"] = True 

        movieEval = getAverageEval(x["_id"])
        x["_movieEval"]= round(movieEval,1)

        x["_oldScore"] = x["_score"]/elastic_max_score
       
        x["_userEval"] = userEval
        
        new_score = calculate_score(x["_oldScore"],userEval/5,movieEval/5)
        
        x["_score"] = new_score
        

    sorted_movies = sorted(res["hits"]["hits"], key=lambda x: x["_score"],reverse=True)
   
    return sorted_movies

        


if __name__=="__main__":
    script_dir = os.path.dirname(__file__)
    os.chdir(script_dir)
    es = Elasticsearch(host="localhost",port=9200,timeout=500)
    load_data("ratings.csv","ratings")
    a = input("Search for a movie: ")
    b = int(input("User ID: "))
    res = search_movie(phrase=a,userId=b,num=10)
    print_movies(res)
