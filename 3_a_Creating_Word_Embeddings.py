import pandas as pd
import os
from gensim.models import Word2Vec 
from nltk.tokenize import RegexpTokenizer
from nltk.corpus import stopwords
import nltk
import string
script_dir = os.path.dirname(__file__)
os.chdir(script_dir)

def load_csv(csv_name):
    curdir = os.path.dirname(__file__)
    csv_file = curdir+"/datasets/"+csv_name
    csv = pd.read_csv(csv_file)
    return csv

#επιστρέφει μια λίστα με τα tokens ενός τίτλου
#δεν περνάει τα stopwords της αγγλικής γλώσσας.
def tokenize(title):
    stopword = stopwords.words('english')
    titleVec=[]
    tokenizer = RegexpTokenizer(r'\w+')
    tokens = tokenizer.tokenize(title)
    tokens = [word for word in tokens if word not in stopword]
    tokens = [word.lower() for word in tokens if word.isalpha()]
    return tokens


def create_WE_model(movies):
    titles = movies["title"].values
    #Την πρ΄ωτη φορα πρέπει να τρέξουν
    nltk.download('punkt')
    nltk.download('stopwords')
    #==============================
    titleVec=[]   
    for title in titles: 
        tokens = tokenize(title)
        if len(tokens)==0:
            tokens = title
        titleVec.append(tokens)
    model = Word2Vec(titleVec,min_count=1,size = 100)
    return model


def get_genres(movies):
    genres=[]
    for index,row in movies.iterrows():
        tmp = row["genres"].split("|")
        for x in tmp:
            if x not in genres:
                genres.append(x)
    return genres

#δημιουργεί το dataset με τα vectors των τίτλων και το One hot encoding των genres.
def create_dataset(model,movies):
    allGenres = get_genres(movies)
    vec =[x for x in range(100)]
    Cols = ["movieId"] + vec+ allGenres

    rowList = []
    n=0
    for index,row in movies.iterrows():
        title = tokenize(row["title"])
        if len(title) == 0:
            title = row["title"]
        genres = row["genres"].split("|")
        if len(title)==1:
            vec_title = model[title[0]]
        else:
            vec_title = sum([model[x] for x in title])
        
        entry = dict((x,0) for x in Cols)
        entry["movieId"] = row["movieId"]
        for x in range(100):
            entry[x]=vec_title[x]
        for x in allGenres:
            if x in genres:
                entry[x] = 1
            else:
                entry[x] = 0    
        rowList.append(entry) 
    data = pd.DataFrame(data = rowList)
    data = data.drop(["(no genres listed)"],axis=1)
    return data
    
    
if __name__=="__main__":
    movies = load_csv("movies.csv")
    model = create_WE_model(movies)
    data = create_dataset(model,movies)
    ##Αποθηκευση μοντελου και dataset
    data.to_csv("datasets/encodedMovies.csv",index=False)
    model.wv.save_word2vec_format('models/W2V_model.bin')