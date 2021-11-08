import pandas as pd
import os
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
import numpy as np


def load_csv(csv_name):
    curdir = os.path.dirname(__file__)
    csv_file = curdir+"/datasets/"+csv_name
    csv = pd.read_csv(csv_file)
    return csv


def plot_graphs(n,inertias):
    dx = np.diff(n)
    dy = np.diff(inertias)
    slopes = dy/dx
    plt.figure()
    plt.plot(n,inertias)
    plt.grid()
    plt.title("Inertia")
    plt.figure()
    plt.plot(slopes)
    plt.title("Slope")
    plt.grid()
    plt.show()
if __name__=="__main__":
    ratings = load_csv("averageRatings.csv")
    #ratings.info()
    #υπαρχουν μονο 14 ταινιες χωρίς να αναγράφεται το είδος τους
    #οπότε μια καλή προσέγγιση είναι να διαγραφεί αυτή η στήλη
    ratings.drop('(no genres listed)',axis='columns', inplace=True)

    #Συμπληρώνεται η κάθε κενή τιμη με τον μέσο όρο της στήλης στην οποία βρίσκεται
    
    ratings.fillna(ratings.mean(),inplace=True)
    idCol = ratings["UserId"]
    ratings.drop("UserId",axis="columns",inplace =True)
    
    #KMEANS για 30 διαφορετικούς αριθμούς cluster
    inertias = []
    for i in range(1,30):
        model = KMeans(n_clusters = i)
        model.fit(ratings)
        inertias.append(model.inertia_)
    n = [x for x in range(1,i+1)]
    plot_graphs(n,inertias)
    #Απο τα διαγραμματα βλεπω ότι η αδράνεια μειώνεται εκθετικά με τον αριθμό τον clusters
    #Ψάχνω την τιμή εκείνη που η κλίση της καμπύλης σταματάει να αλλάζει πολύ
    #καθώς παραπάνω clusters από αυτή την τιμή δεν θα μου δώσουν επιπλέον πληφορορία
    #επιλέγω αριθμό clusters = 10
    model = KMeans(n_clusters = 10).fit(ratings)
    clusters = pd.DataFrame()
    clusters["userId"] = idCol
    clusters["cluster"] = model.labels_
    clusters.to_csv("datasets/clusters.csv",index=False)
    
    