import os
import time
import pandas as pd
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

#to find a path of a particular check point
def checkp(path,checkpoint):
    l_files = os.listdir(path) # getting all the contents of a particular folder
    for f_name in l_files:# Iterating over all the files
        if '.' not in f_name and checkpoint not in f_name:
          l_files2 = os.listdir(path +'/'+f_name)
          for f_name2 in l_files2:# Iterating over all the files
            if '.' not in f_name2 and checkpoint in f_name2:
              return(path +'/'+f_name+'/'+f_name2)
        
# For getting detailed report on the entire dataset
def detailed_report_writer(name,f_name,count_logs,unique_logs_types,unique_logs_type_messages,out_path):
    f2 = open(out_path+name, "a")
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

# For extracting data from each log file
def log_read(f_name,out_path,start,stop):
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
            if start == "" and stop == "":
                count_logs+=1
                arr.append(line_no)
                dates.append(line[0:10])
                log.append(line[24:29])
                unique_logs_types.add(line[24:29]) # finding the unique types of logs in the log file
            elif pd.to_datetime(start) <= pd.to_datetime(line[:22]) and pd.to_datetime(stop) >= pd.to_datetime(line[:22]):
                count_logs+=1
                arr.append(line_no)
                dates.append(line[0:10])
                log.append(line[24:29])
                unique_logs_types.add(line[24:29]) # finding the unique types of logs in the log file
            elif pd.to_datetime(start) <= pd.to_datetime(line[:22]):
                count_logs+=1
                arr.append(line_no)
                dates.append(line[0:10])
                log.append(line[24:29])
                unique_logs_types.add(line[24:29]) # finding the unique types of logs in the log file
            elif pd.to_datetime(stop) >= pd.to_datetime(line[:22]):
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
    if len(unique_logs_types)!= 0:
        detailed_report_writer("Detailed_Report_log.txt",f_name,count_logs,unique_logs_types,unique_logs_type_messages,out_path) # to create the detatiles report
    file1.close()
    return([count_logs,unique_log_type_counts,dates,log,unique_logs_type_messages])

# For extracting data from each tomcat log file
def tomcat_read(f_name,out_path,start,stop):
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
            if start == "" and stop == "":
                count_logs+=1
                arr.append(line_no)
                dates.append(line[line.find(' - - [')+6:].split(':', 1)[0])
                log.append(line[line.find('00]')+5:].split(' ', 1)[0])
                unique_logs_types.add(line[line.find('00]')+5:].split(' ', 1)[0])
            elif pd.to_datetime(start) <= pd.to_datetime(line[line.find(' - - [')+6:].split(':', 1)[0]) and pd.to_datetime(stop) >= pd.to_datetime(line[line.find(' - - [')+6:].split(':', 1)[0]):
                count_logs+=1
                arr.append(line_no)
                dates.append(line[line.find(' - - [')+6:].split(':', 1)[0])
                log.append(line[line.find('00]')+5:].split(' ', 1)[0])
                unique_logs_types.add(line[line.find('00]')+5:].split(' ', 1)[0])
            elif pd.to_datetime(start) <= pd.to_datetime(line[line.find(' - - [')+6:].split(':', 1)[0]):
                count_logs+=1
                arr.append(line_no)
                dates.append(line[line.find(' - - [')+6:].split(':', 1)[0])
                log.append(line[line.find('00]')+5:].split(' ', 1)[0])
                unique_logs_types.add(line[line.find('00]')+5:].split(' ', 1)[0])
            elif pd.to_datetime(stop) >= pd.to_datetime(line[line.find(' - - [')+6:].split(':', 1)[0]):
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
    if len(unique_logs_types)!= 0:
        detailed_report_writer("Detailed_Report_tomcat.txt",f_name,count_logs,unique_logs_types,unique_logs_type_messages,out_path) # to create the detatiles report
    file1.close()
    return([count_logs,unique_log_type_counts,dates,log,unique_logs_type_messages])

def extracter(path,f_name,target):
    try:                # this will create the folder if the folder is missing where the log files will be kept
        shutil.copy(path+'/'+f_name, target+f_name)
    except OSError as error: 
        os.mkdir(target)
        shutil.copy(path+'/'+f_name, target+f_name)

#For traversing the complete file structure
def log_finder(path,total_log_files,total_logs,file_log,unique_log_type_count,dates,log,visited,out_path,unique_logs_type_messages,start,stop):
  l_files = os.listdir(path) # getting all the contents of a particular folder
  
  for f_name in l_files:# Iterating over all the files
    if '.' not in f_name and f_name != "tomcat-logs": # only checking for log folders
        temp = log_finder(path +'/'+f_name,total_log_files,total_logs,file_log,unique_log_type_count,dates,log,visited,out_path,unique_logs_type_messages,start,stop)
        total_log_files=temp[0]
        total_logs=temp[1]
        file_log=temp[2]
        unique_log_type_count = temp[3]
        dates = temp[4]
        log = temp[5]
        visited = temp[6]
        unique_logs_type_messages = temp[7]
    elif '.log' in f_name and f_name not in visited : # only checking for unique log files
      visited.append(f_name)
      info = log_read(path+'/'+f_name,out_path,start,stop)
      extracter(path,f_name,out_path+'Uniquelog/')# this function copies unique files to a particular location
      count_logs=info[0]
      file_log[path+'/'+f_name]=info[1]
      dates.extend(info[2])
      log.extend(info[3])
      for t in info[4].keys():
        if t not in unique_logs_type_messages: # to keep all the unique log messages in a log files by type
          unique_logs_type_messages[t]=set()
        for m in info[4][t]:
          unique_logs_type_messages[t].add(m)
      for k in info[1].keys(): # to keep calculating each log type count in the entire dataset
        if k in unique_log_type_count.keys():
            unique_log_type_count[k] = unique_log_type_count[k]+info[1][k]
        else:
            unique_log_type_count[k] = info[1][k]
      total_logs +=count_logs # count the total number of logs
      total_log_files +=1 # count the total number of log files
  return([total_log_files,total_logs,file_log,unique_log_type_count,dates,log,visited,unique_logs_type_messages])

#For traversing the complete file structure
def tomcat_finder(path,total_log_files,total_logs,file_log,unique_log_type_count,dates,log,visited,out_path,unique_logs_type_messages,start,stop):
  l_files = os.listdir(path) # getting all the contents of a particular folder
  for f_name in l_files:# Iterating over all the files
    if '.' not in f_name and f_name != "log": # only checking for tomcat folders
        temp = tomcat_finder(path +'/'+f_name,total_log_files,total_logs,file_log,unique_log_type_count,dates,log,visited,out_path,unique_logs_type_messages,start,stop)
        total_log_files=temp[0]
        total_logs=temp[1]
        file_log=temp[2]
        unique_log_type_count = temp[3]
        dates = temp[4]
        log = temp[5]
        visited = temp[6]
        unique_logs_type_messages = temp[7]
    elif '.log' in f_name and f_name not in visited: # only checking for unique tomcat log files
      visited.append(f_name)
      info = tomcat_read(path+'/'+f_name,out_path,start,stop)
      extracter(path,f_name,out_path+'Uniquetomcat/') # this function copies unique files to a particular location
      count_logs=info[0]
      file_log[path+'/'+f_name]=info[1]
      dates.extend(info[2])
      log.extend(info[3])
      for t in info[4].keys():
        if t not in unique_logs_type_messages: # to keep all the unique log messages in a log files by type
          unique_logs_type_messages[t]=set()
        for m in info[4][t]:
          unique_logs_type_messages[t].add(m)
      for k in info[1].keys(): # to keep calculating each log type count in the entire dataset
        if k in unique_log_type_count.keys():
            unique_log_type_count[k] = unique_log_type_count[k]+info[1][k]
        else:
            unique_log_type_count[k] = info[1][k]
      total_logs +=count_logs # count the total number of logs
      total_log_files +=1 # count the total number of log files
  return([total_log_files,total_logs,file_log,unique_log_type_count,dates,log,visited,unique_logs_type_messages])

#gives a summery of the required log types
def summary(record_mod):
    summ=[] #to return the summary of max, min, avg, std values of the count of each type of log
    for i in record_mod.drop('Dates', axis=1).columns.values:
        b = record_mod[i].describe()
        information = b.to_dict()
        s = "The highest number of "+str(i)+" logs in the time period was " + str(information['max'])+" record on " + str(record_mod["Dates"].loc[record_mod[i].idxmax()])[:10] + ". It has an average count of "+str(round(information['mean']))+".\nThe lowest number of "+ str(i)+" logs in the time period was "+str(information['min'])+" record_moded on "+str( record_mod["Dates"].loc[record_mod[i].idxmin()])[:10]
        summ.append(s)
    return(summ)

# creates a csv file date wise count of each log type for further analysis
def summarizer(Dates,Logs,name,out_path):
    record = pd.DataFrame({"Dates":[],"Log":[]}) #to keep track of datewise log
    record['Dates'] = pd.to_datetime(Dates)
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
    record_mod.to_csv(out_path+name,index=False)
    return(summary(record_mod))

# this function generates the summery report 
def report_writer(total,name,summ,out_path):
    f1 = open(out_path+name, "a")
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

# For getting detailed report on a unique log type
def unique_data_accumulator(out_path,total,table):
    print("The log types whose detailed report can be seperately srecoe") 
    for u in total[3].keys():
        print(u," ")
    log_type=input("Enter your choice and if not interested enter none :-")
    for u in total[3].keys():
        if log_type.upper() in u:
            cs = pd.read_csv(out_path+table)
            f1 = open(out_path+"detailed_"+log_type+".txt", "a")
            f1.write("\nTotal number of "+u+" logs in the data set are:-"+str(cs[u].sum())+
                     "\nThe highest count of "+u+" logs in the time period was " + str(cs[u].max())+
                     " record on" + str(cs["Dates"].loc[cs[u] == cs[u].max()])[7:17] + 
                     ".\nIt has an average count of "+str(round(cs[u].mean()))+
                     " and a standard deviation of "+str(round(cs[u].std()))+
                     ".\nThe count number of "+ u+" logs in the time period was "+str(cs[u].min())+
                     " record_moded on"+str( cs["Dates"].loc[cs[u].min()]))
            f1.write("\nThe Unique Messages of "+u+" type are:\n")
            for m in total[7][u]:
                f1.write(m)
            f1.write("\nThe "+u+" log count pre file:\n")
            dict1 = total[2]
            for file in dict1.keys():
                dict2 = dict1[file]
                try:
                    f1.write("\n"+file+" :- ")
                    f1.write("\n"+u+" : "+str(dict2[u]))
                except:
                    pass
            f1.close()

def log_prog(path,out_path,detail,start,stop,checkpoint):
    file_log = {} # to keep track of logs per file
    visited = [] # to keep track of the unique log files
    unique_log_type_count = {} #to keep count of all the unique log type counts
    dates=[] #To get all the dates of the logs
    log = [] #To get all the log
    unique_logs_type_messages={} # dictionary to keep track of unique log messages by type

    if checkpoint !="":
        path = checkp(path,checkpoint)
    
    total = log_finder(path,0,0,file_log,unique_log_type_count,dates,log,visited,out_path,unique_logs_type_messages,start,stop)
    summ =summarizer(total[4],total[5],"log.csv",out_path)
    report_writer(total,"Report_log.txt",summ,out_path)
    if detail == 1:
        unique_data_accumulator(out_path,total,"log.csv")

def tomcat_prog(path,out_path,detail,start,stop,checkpoint):
    file_log = {} # to keep track of logs per file
    visited = [] # to keep track of the unique log files
    unique_log_type_count = {} #to keep count of all the unique log type counts
    dates=[] #To get all the dates of the logs
    log = [] #To get all the log
    unique_logs_type_messages={} # dictionary to keep track of unique log messages by type

    if checkpoint !="":
        path = checkp(path,checkpoint)
    
    total = tomcat_finder(path,0,0,file_log,unique_log_type_count,dates,log,visited,out_path,unique_logs_type_messages,start,stop)
    summ = summarizer(total[4],total[5],"tomcat.csv",out_path)
    report_writer(total,"Report_tomcat.txt",summ,out_path)
    if detail == 1:
        unique_data_accumulator(out_path,total,"tomcat.csv")

def main_caller(path,log_type,out_path,detail,start,stop,checkpoint):
    if log_type == 'log':
        log_prog(path,out_path,detail,start,stop,checkpoint)
    elif log_type == 'tomcat':
        tomcat_prog(path, out_path,detail,start,stop,checkpoint)
    else:
        log_prog(path,out_path,detail,start,stop,checkpoint)
        tomcat_prog(path,out_path,detail,start,stop,checkpoint)


# settingup the parser
parser = argparse.ArgumentParser(description="Toolkit for the Server log dataset")
parser.add_argument("--t", type=str,default="both",choices=['log','tomcat','both'],help="Get report for either log or tomcat or both log files types")
parser.add_argument("--in_path", type=str,default = "D:/Sajib da/Server_data_work/logs",help="Get input path of log files")
parser.add_argument("--out_path", type=str,default = "D:/Sajib da/Server_data_work/",help="Get output path of log files")
parser.add_argument("--detail", type=int,default = 0,choices=[0,1],help="Use 1 for detailed report on a particular log type")
parser.add_argument("--startdate", type=str,default ="",help="To get reports starting from a particular date")
parser.add_argument("--stopdate", type=str,default ="",help="To get reports till a particular date")
parser.add_argument("--checkpoint", type=str,default ="",help="To get reports only from a check point")
args = parser.parse_args()

main_caller(args.in_path,args.t,args.out_path,args.detail,args.startdate,args.stopdate,args.checkpoint)
