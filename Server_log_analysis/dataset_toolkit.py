import os
import time
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
import shutil
import argparse

#to check if a stirng is an ipv4 address.
def is_ip(s):
    pieces = s.split('.')
    for p in pieces:
        if int(p[:3])>=0 and int(p[:3])<=255:
            return True
        else:
            return False
        
# For getting detailed report on the entire dataset
def detailed_report_writer(name,f_name,count_logs,unique_logs_types,unique_logs_type_messages):
    f2 = open(name, "a")
    f2.write("\n============================================================================================================================================================================================================")
    f2.write("\n************************** "+f_name+" **************************")
    f2.write("\nTotal number of logs in the file "+ f_name +" is / are "+str(count_logs))
    f2.write("\nUnique types of logs in this file are :- ")
    for u in unique_logs_types:
        f2.write(str(u))
    f2.write("\nThe Unique logs messages by type are:- ")
    for k in unique_logs_type_messages.keys():
        f2.write("\nThe Following Message Type : "+k+"\n")
        for m in unique_logs_type_messages[k]:
            f2.write(m)
    f2.close()

    # For getting detailed report on a unique log type ********************************************************************************************
def unique_data(f_name,unique_log_type_count,unique_logs_type_messages):
    print("\nThe unique log types are:- ")
    for u in unique_log_type_count.keys():
        print(" "+u+" : ")
    option  = input("Enter your choice to get detailed report on one of these log types")
    f2 = open("Detailed_Report_"+option+".txt", "a")
    f2.write("\n============================================================================================================================================================================================================")
    f2.write("\n************************** "+f_name+" **************************")
    for u in unique_log_type_count.keys():
        if option in u:
            f2.write("\nTotal number of "+option+" logs in the data set are" + unique_log_type_count[u]+"\nThe unique "+option+" logs are:-")
            f2.write(unique_logs_type_messages[u])
            
# For extracting data from each log file
def log_read(f_name):
    file1 = open(f_name, 'r')
    Lines = file1.readlines()
    count_logs = 0 # to count the no. of log files
    line_no=0 # to keep track of the lines in the file
    arr=[] # to record the line numbers where each log exists
    unique_logs_types=set() # set to keep track of unique types of logs
    unique_logs_type_messages={} # dictionary to keep track of unique log messages by type
    unique_log_type_counts = {} # dictionary to keep count the number of occurances of each type of log
    dates=[] #To get all the dates of the logs
    log = [] #To get all the log
    
    for line in Lines:
        try:
            time.strptime(line[:22], '%Y-%m-%d %H:%M:%S.%f') # checking if a line is a log file # 'Explanation needed ?'
        except ValueError:
            pass
        else:
            count_logs+=1
            arr.append(line_no)
            dates.append(line[0:10])
            log.append(line[24:29])
            unique_logs_types.add(line[24:29]) # finding the unique types of logs in the log file
        line_no+=1

    for t in unique_logs_types:
        c=0
        for a in arr: # to count the number of occurances of each type of log
            l = Lines[a]
            if t == l[24:29]:
                c+=1
                unique_log_type_counts[t] = c
                if t not in unique_logs_type_messages: # finding all the unique log messages in a log file by type
                    unique_logs_type_messages[t]=set()
                unique_logs_type_messages[t].add(l[l.find(' :'):])
    detailed_report_writer("Detailed_Report_log.txt",f_name,count_logs,unique_logs_types,unique_logs_type_messages) # to create the detatiles report
    file1.close()
    return([count_logs,unique_log_type_counts,dates,log])

# For extracting data from each tomcat log file
def tomcat_read(f_name):
    file1 = open(f_name, 'r')
    Lines = file1.readlines()
    count_logs = 0 # to count the no. of log files
    line_no=0 # to keep track of the lines in the file
    arr=[] # to record the line numbers where ecah log exists
    unique_logs_types=set() # set to keep track of unique types of logs
    unique_logs_type_messages={} # dictionary to keep track of unique log messages by type
    unique_log_type_counts = {} # dictionary to keep count the number of occurances of each type of log
    dates=[] #To get all the dates of the logs
    log = [] #To get all the log
    
    for line in Lines:        
        if is_ip(line[:15]): #to check for logs in the tomcat_log files
            count_logs+=1
            arr.append(line_no)
            dates.append(line[line.find(' - - [')+6:].split(':', 1)[0])
            log.append(line[line.find('00]')+5:].split(' ', 1)[0])
            unique_logs_types.add(line[line.find('00]')+5:].split(' ', 1)[0])
        line_no+=1
    for t in unique_logs_types:
        c=0
        for a in arr: # to count the number of occurances of each type of log
            l = Lines[a]
            if t == l[l.find('00]')+5:].split(' ', 1)[0]:
                c+=1
                unique_log_type_counts[t] = c
                if t not in unique_logs_type_messages: # finding all the unique log messages in a log file by type
                    unique_logs_type_messages[t]=set()
                unique_logs_type_messages[t].add(l[l.find('00]')+5:].split(' ', 1)[1])
    detailed_report_writer("Detailed_Report_tomcat.txt",f_name,count_logs,unique_logs_types,unique_logs_type_messages) # to create the detatiles report
    file1.close()
    return([count_logs,unique_log_type_counts,dates,log])

def log_extracter(path,f_name):
    target = 'D:/Sajib da/Server_data_work/Uniquelog/' #path were all the uniquelog files will be copied to.
    try:                # this will create the folder if the folder is missing where the log files will be kept
        shutil.copy(path+'/'+f_name, target+f_name)
    except OSError as error: 
        os.mkdir(target)
        shutil.copy(path+'/'+f_name, target+f_name)

def tomcat_extracter(path,f_name):
    target = 'D:/Sajib da/Server_data_work/Uniquetomcat/' #path were all the uniquelog files will be copied to.
    try:                # this will create the folder if the folder is missing where the log files will be kept
        shutil.copy(path+'/'+f_name, target+f_name)
    except OSError as error: 
        os.mkdir(target)
        shutil.copy(path+'/'+f_name, target+f_name)

#For traversing the complete file structure
def log_finder(path,total_log_files,total_logs,file_log,unique_log_type_count,dates,log,visited):
  l_files = os.listdir(path) # getting all the contents of a particular folder
  for f_name in l_files:# Iterating over all the files
    if '.' not in f_name and f_name != "tomcat-logs": # only checking for log folders
      temp = log_finder(path +'/'+f_name,total_log_files,total_logs,file_log,unique_log_type_count,dates,log,visited)
      total_log_files=temp[0]
      total_logs=temp[1]
      file_log=temp[2]
      unique_log_type_count = temp[3]
      dates = temp[4]
      log = temp[5]
      visited = temp[6]
    elif '.log' in f_name and f_name not in visited : # only checking for unique log files
      visited.append(f_name)
      info = log_read(path+'/'+f_name)
      log_extracter(path,f_name)# this function copies unique files to a particular location
      count_logs=info[0]
      file_log[path+'/'+f_name]=info[1]
      dates.extend(info[2])
      log.extend(info[3])
      for k in info[1].keys(): # to keep calculating each log type count in the entire dataset
        if k in unique_log_type_count.keys():
            unique_log_type_count[k] = unique_log_type_count[k]+info[1][k]
        else:
            unique_log_type_count[k] = info[1][k]
      total_logs +=count_logs # count the total number of logs
      total_log_files +=1 # count the total number of log files
  return([total_log_files,total_logs,file_log,unique_log_type_count,dates,log,visited])

#For traversing the complete file structure
def tomcat_finder(path,total_log_files,total_logs,file_log,unique_log_type_count,dates,log,visited):
  l_files = os.listdir(path) # getting all the contents of a particular folder
  for f_name in l_files:# Iterating over all the files
    if '.' not in f_name and f_name != "log": # only checking for log folders
      temp = tomcat_finder(path +'/'+f_name,total_log_files,total_logs,file_log,unique_log_type_count,dates,log,visited)
      total_log_files=temp[0]
      total_logs=temp[1]
      file_log=temp[2]
      unique_log_type_count = temp[3]
      dates = temp[4]
      log = temp[5]
      visited = temp[6]
    elif '.log' in f_name and f_name not in visited: # only checking for unique tomcat log files
      visited.append(f_name)
      info = tomcat_read(path+'/'+f_name)
      tomcat_extracter(path,f_name) # this function copies unique files to a particular location
      count_logs=info[0]
      file_log[path+'/'+f_name]=info[1]
      dates.extend(info[2])
      log.extend(info[3])
      for k in info[1].keys(): # to keep calculating each log type count in the entire dataset
        if k in unique_log_type_count.keys():
            unique_log_type_count[k] = unique_log_type_count[k]+info[1][k]
        else:
            unique_log_type_count[k] = info[1][k]
      total_logs +=count_logs # count the total number of logs
      total_log_files +=1 # count the total number of log files
  return([total_log_files,total_logs,file_log,unique_log_type_count,dates,log,visited])

#gives a summery of the required log types
def summary(record_mod):
    summ=[] #to return the summary of max, min, avg, std values of the count of each type of log
    for i in record_mod.drop('Dates', axis=1).columns.values:
        b = record_mod[i].describe()
        information = b.to_dict()
        s = "The highest number of "+str(i)+" logs in the time period was " + str(information['max'])+" record on" + str(record_mod["Dates"].loc[record_mod[i].idxmax()]) + ". It has an average count of "+str(round(information['mean']))+" and a standard deviation of "+str(round(information['std']))+".\nThe lowest number of "+ str(i)+" logs in the time period was "+str(information['min'])+" record_moded on"+str( record_mod["Dates"].loc[record_mod[i].idxmin()])
        summ.append(s)
    return(summ)

# creates a csv file date wise count of each log type for further analysis
def summarizer(Dates,Logs,name):
    record = pd.DataFrame({"Dates":[],"Log":[]}) #to keep track of datewise log
    record['Dates'] = Dates
    record["Log"] = Logs
    a = record.groupby(['Dates', 'Log']).size() #To find count of each log type datewise 
    times = list(record['Dates'].unique())
    log = list(record['Log'].unique())
    meta= a.to_dict()
    record_mod = pd.DataFrame({"Dates":times}) 
    for i in log:
        record_mod[i]=0
    for k in meta.keys():
        record_mod.loc[record_mod['Dates']==k[0], k[1]] = meta[k]
    record_mod.to_csv(name,index=False)
    return(summary(record_mod))

# this function generates the summery report 
def report_writer(total,name,summ):
    f1 = open(name, "a")
    f1.write("---------------------------------------------------------------------------------- Report / Summery ----------------------------------------------------------------------------------\n")
    f1.write("\nTotal number of log file/s in the log folders of the data set " + str(total[0])+
             "\nTotal number of logs in the log folders of the dataset "+str(total[1])+
             "\nThe unique log types and their total count in the log folders of the datasets are :-")
    for u in total[3].keys():
        print(" "+u+" : "+str(total[3][u]))
        f1.write(" "+u+" : "+str(total[3][u])+",")
    print("A summary of max, min, avg, std values of the count of each type of log:-")
    f1.write("\nA summary of max, min, avg, std values of the count of each type of log:-")
    for s in summ:
        print(s)
        f1.write("\n"+s)
    dict1 = total[2]
    for file in dict1.keys():
        dict2 = dict1[file]
        f1.write("\n"+file+" :- ")
        for record in dict2.keys():
            f1.write("\n"+record+" : "+str(dict2[record]))
    f1.close()

def log_prog(path):
    file_log = {} # to keep track of logs per file
    visited = [] # to keep track of the unique log files
    unique_log_type_count = {} #to keep count of all the unique log type counts
    dates=[] #To get all the dates of the logs
    log = [] #To get all the log
    
    total = log_finder(path,0,0,file_log,unique_log_type_count,dates,log,visited)
    summ =summarizer(total[4],total[5],"log.csv",)
    report_writer(total,"Report_log.txt",summ)

def tomcat_prog(path):
    file_log = {} # to keep track of logs per file
    visited = [] # to keep track of the unique log files
    unique_log_type_count = {} #to keep count of all the unique log type counts
    dates=[] #To get all the dates of the logs
    log = [] #To get all the log
    
    total = tomcat_finder(path,0,0,file_log,unique_log_type_count,dates,log,visited)
    summ = summarizer(total[4],total[5],"tomcat.csv")
    report_writer(total,"Report_tomcat.txt",summ)

def main_caller(path,log_type):
    if log_type == 'log':
        log_prog(path)
    elif log_type == 'tomcat':
        tomcat_prog(path)
    else:
        log_prog(path)
        tomcat_prog(path)


# settingup the parser
parser = argparse.ArgumentParser(description="Toolkit for the Server log dataset")
parser.add_argument("--t", type=str,default="both",choices=['log','tomcat','both'],help="Get report for either log or tomcat or both log files types")
parser.add_argument("--p", type=str,default = "D:/Sajib da/Server_data_work/logs",help="Get input path of log files")
args = parser.parse_args()

main_caller(args.p,args.t)