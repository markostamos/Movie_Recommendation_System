import pandas as pd
import os
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
script_dir = os.path.dirname(__file__)
os.chdir(script_dir)
import keras
import numpy as np
from sklearn.model_selection import train_test_split
import matplotlib.pyplot as plt



def load_csv(csv_name):
    curdir = os.path.dirname(__file__)
    csv_file = curdir+"/datasets/"+csv_name
    csv = pd.read_csv(csv_file)
    return csv

#για κάθε :user δημιουργεί το training dataset εισόδου καθώς και την επιθυμητή έξοδο
#δηλαδή τα ratings.
def getUserTrainData(userId,ratings,movies):
    ratings = ratings[ratings["userId"]==userId]
    movieIDs = list(ratings["movieId"])
    movies = movies[movies["movieId"].isin(movieIDs)]
    movies = movies.sort_values(by = 'movieId')
    X_train = movies.drop(["movieId"],axis=1)
    ratings = ratings.sort_values(by = 'movieId')
    ratings = ratings["rating"]
    return X_train,ratings


if __name__=="__main__":
    movies = load_csv("encodedMovies.csv")
    ratings = load_csv('ratings.csv')
    #για κάθε έναν απο τους 671 χρήστες
    for i in range(1,672):
        
        X,y =getUserTrainData(i,ratings,movies)
  
        X = keras.utils.normalize(X,axis=1)
  

        #X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)
        X_train = X
        y_train = y
        model = keras.Sequential()
        #119 είσοδοι (100 το μήκος του κάθε διανύσματος τίτλου, +19  οι κατηγορίες )
        model.add(keras.layers.Dense(119,activation='relu',input_shape=(119,)))
        model.add(keras.layers.Dense(64,activation='relu'))
        model.add(keras.layers.Dense(32,activation='relu'))
        #1 έξοδος που αναπαριστά την βαθμολογία
        #
        model.add(keras.layers.Dense(1))
        #επειδή επιλέχθηκε 1 έξοδος για loss function επιλέχθηκε το
        #mean_squared_error έναντι του mean_absolute_error ώστε να "τιμωρεί" το νευρωνικό παραπάνω για τιμές που απέχουν
        #πολύ απο την επιθυμητή
        model.compile(optimizer='adam',loss='mean_squared_error')
        
        #από την ανάλυση που φαίνεται στα σχόλια κώδικα βρέθηκε ότι χρειάζονται περίπου
        #20 epochs για να εκπαιδευτεί το κάθε νευρωνικό χωρίς να έχουμε overfitting
        #(πιο συγκεκριμένα κοιτάξαμε τις καμπύλες σφάλματος για τα training και test data)
        #για περισσότερα από 20-25 epochs για αρκετούς χρήστες οι καμπύλες αυτές απομακρύνονται
        #δηλαδη η καμπύλη για το training dataset συνεχίζει να μειώνεται ένω αυτή για το
        #test σετ είτε μένει σταθερή είτε αυξάνεται --> δείγμα overfitting.
        history = model.fit(X_train,y_train,epochs=20,verbose=0)
       
        # plt.plot(history.history['loss'],label="train")
        # plt.plot(history.history['val_loss'],label='test')
        # plt.show()
        print("============= training model ",i," of 671 ===========")
        path ='models/'+str(i)
        #τέλος αποθηκεύεται το κάθε μοντέλο
        model.save(path)
        print("")



        