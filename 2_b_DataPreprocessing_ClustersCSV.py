import pandas as pd
import os
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
import numpy as np

#Αυτό είναι το δεύτερο κομμάτι της 3ης άσκησης, πρακτικά δημιουργεί το αρχείο clusters.csv
#όπου δίχνει για κάθε χρήστη σε ποιο cluster ανήκει.
#όπως και πριν στα datasets έχουμε το αποτέλεσμα δηλαδή το "clusters.csv" οπότε δεν χρειάζεται να τρέξει
#το συγκεκριμένο script.
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
    #το συγκεκριμένο dataset αφορά την μέση αξιολόγηση
    #του κάθε χρήστη για κάθε είδος ταινιών
    #επομένως αν κάποιος χρήστης δεν έχει δει καμία ταινία
    #απο ένα συγκεκριμένο είδος θα μπορούσαμε απλώς να διαγράψουμε
    #τις γραμμές που έχουν κάποια κενή τιμή
    #Ωστόσο αυτές είναι πάρα πολλές
    #Επομένως η καλύτερη επιλογή είναι να συμπληρώσουμε την κάθε κενή
    #τιμη με τον μέσο όρο της στήλης στην οποία βρίσκεται
    #Προτιμάται αυτός ο τρόπος έναντι τιμών όπως (median,0 η max)
    #καθώς δίνει επιπλέον πληροφορία για τα δεδομένα
    #Π.χ αν ο μέσος όρος των βαθμολογιών των ταινιών "Adventure"
    #είναι πολύ μεγάλος σημαίνει ότι το dasatet περιέχει πολύ καλές ταινίες 
    #επομένως έμμεσα προτείνουμε στον χρήστη που δεν εχει δει τέτοιες ταινίες να τις δει
    ratings.fillna(ratings.mean(),inplace=True)
    idCol = ratings["UserId"]
    ratings.drop("UserId",axis="columns",inplace =True)
    
    #Δοκιμάζω τον KMEANS για 30 διαφορετικούς αριθμούς cluster
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
    #ισα ισα θα μειώσουν τον αριθμό των χρηστών ανα cluster επομένως για μια αναζήτηση
    #ταινιας θα αυξηθεί η πιθανότητα να μην έχει βαθμολογήσει κανένας χρήστης απο τον cluster
    #την συγκεκριμένη ταινια
    #επιλέγω αριθμό clusters = 10
    model = KMeans(n_clusters = 10).fit(ratings)
    clusters = pd.DataFrame()
    clusters["userId"] = idCol
    clusters["cluster"] = model.labels_
    clusters.to_csv("datasets/clusters.csv",index=False)
    
    