import matplotlib.pyplot as mp #import packages
import pandas as pd
import seaborn as sb

data= pd.read_csv("dlbcl-fl.csv") # load the dataset
data2 = data.copy().drop("class",axis=1) #keeping a separate copy of only the features

def most_influence(data2,std1): # To find the influential attributes / features
    s=[]
    cols = []
    for v in std1:
        s.append(v)
    s.sort(reverse=True)
    limit = int(input("Enter number:"))
    for i in range(limit):
        cols.append(data2.columns[std1.index(s[i])])
        i+=1
    return(cols)

print("The 'dlbcl-fl.csv' is loaded.")
choice = 0
std1 = [] #to store the standard deviation after performing normalization
while(choice != 100):
    print("\n\n****************************** Main Menu ******************************")
    print("\nYou can perform the following operations with this data set:-")
    print("Press 1 #View few rows from the data set \nPress 2 #View the number of Rows and Columns of the dataset")
    print("Press 3 #View the unique classes \nPress 4 to check for missing values\nPress 5 #Normalization the dataset")
    print("Press 6 #To find the most n influencial attributes / features that influence the class outcome (only use after normalizing the dataset)")
    print("Press 7 #To find the most n influencial attributes / features that influence the class outcome and create a csv file for the same (only use after normalizing the dataset)")
    print("Press 8 #Reset the dataset ")
    print("Press 100 #To quit\n")
    choice = int(input("\nPlease enter your choice: "))

    if choice == 1:
        print(data.head())
    elif choice == 2:
        print("Total no. of Rows "+str(len(data))+"\nTotal no. of Columns "+str(len(data.columns)))
    elif choice == 3:
        print(data["class"].unique())
    elif choice == 4:
        print(data.isna().values.any()) # checking if there is a missing data
    elif choice == 5:
        sub = 0
        print("***********************")
        print("\nNormalization options:-")
        print("Press 1 #Maximum absolute normalizatiton")
        print("Press 2 #Min Max normalization")
        print("Press 3 #Z score normalization")
        print("Press any other number to return to previous menu")
        sub = int(input("\nPlease enter your choice: "))
        if sub == 1:            #Maximum absolute scaling
            for colm in data2.columns:
                data2[colm] = data2[colm]/data2[colm].abs().max()
                std1.append(data2[colm].std())
            print("The data after normalization: \n")
            print(data2)
        elif sub == 2:          #Min Max normalization
            for colm in data2.columns:
                data
                data2[colm] = (data2[colm]-data2[colm].min())/(data2[colm].max()-data2[colm].min())
                std1.append(data2[colm].std())
            print("The data after normalization: \n")
            print(data2)
        elif sub == 3:          #Z score normalization
            for colm in data2.columns:
                data2[colm] = (data2[colm] - data2[colm].mean())/(data2[colm].std())
                std1.append(data2[colm].std())
            print("The data after normalization: \n")
            print(data2)
    elif choice == 6:
        if len(std1) != 0:
            print(most_influence(data2,std1))
        else:
            print("Without normalizing the dataset this operation is not possible")
    elif choice == 7:
        if len(std1) != 0:
            most_imp = most_influence(data2,std1)
            file = pd.DataFrame()
            for c in most_imp:
                file[c]=list(data2[c])
            file["class"] = list(data["class"])
            file.to_csv("dlbcl_subset.csv",index=False)
            dataplot = sb.heatmap(file.corr(), annot=True)
            mp.show()
        else:
            print("Without normalizing the dataset this operation is not possible")
    elif choice == 8:
        data2 = data.copy().drop("class",axis=1) #keeping a separate copy of only the features
    elif choice == 100:
        print("Quitting...")
    else:
        print("Please enter a valid choice")
    