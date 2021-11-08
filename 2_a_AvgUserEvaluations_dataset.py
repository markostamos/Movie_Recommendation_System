"""Δημιουργεί avg Evaluations του κάθε χρήστη για κάθε είδος ταινίας"""
import pandas as pd
import os


def load_data():
    curdir = os.path.dirname(__file__)
    movies = pd.read_csv(curdir+"/datasets/movies.csv")
    ratings = pd.read_csv(curdir+"/datasets/ratings.csv")
    ratings.drop("timestamp",axis="columns",inplace= True)
    return movies,ratings

#επιστρέφει μια λίστα με όλα τα Genres που υπάρχουν στο dataset
def getAllGenres(movies):
    genres=[]
    for x in movies:
        for y in movies[x]:
            if y not in genres:
                genres.append(y)
    return genres    

#Επιστρέφει μια λίστα με τα genres της ταινίας με movieID
def getGenres(movies,movieId):
    movie = movies.loc[movies["movieId"]==movieId]
    genres = movie["genres"].tolist()
    genres = genres[0].split("|")
    return genres

#χρησιμοποιεί το lookuptable και δημιουργεί τα average ratings για κάθε χρήστη
def getAverageRatings(movies,ratings):
    allGenres = getAllGenres(movies)
    averageRatings = {}
    for index,row in ratings.iterrows():
        userId = row["userId"]
        rating = row["rating"]
        movieId = row["movieId"]
        movieGenres = movies[movieId]
        if userId not in averageRatings:
            genres = {}
            for x in allGenres:
                genres[x] = {"val":0,"n":0}
            averageRatings[userId] = genres
        for x in movieGenres:
            averageRatings[userId][x]["val"]+=rating
            averageRatings[userId][x]["n"] += 1
        
    for x in averageRatings:
        for y in averageRatings[x]:
            
            if averageRatings[x][y]["n"]==0:
                averageRatings[x][y]=None
            else:
                averageRatings[x][y] = averageRatings[x][y]["val"]/averageRatings[x][y]["n"]
    
    averageRatings = pd.DataFrame(data=averageRatings).T
    userIdcol = averageRatings.index
    averageRatings.insert(loc=0,column="UserId",value = userIdcol)
    return averageRatings  

#Δημιουργεί ένα lookup table για πιο γρήγορη πρόσβαση στα genres της κάθε ταινίας ανάλογα με το movieId     
def lookuptable(movies):
    table = {}
    for index,row in movies.iterrows():
        movieId = row["movieId"]
        genres = row["genres"].split("|")
        table[movieId] = genres
    return table

if __name__=="__main__":
    movies,ratings = load_data()
    table = lookuptable(movies)
    averageRatings = getAverageRatings(movies=table,ratings=ratings)
    #αποθηκεύει το csv αρχείο.
    averageRatings.to_csv("datasets/averageRatings.csv",index=False)
    
