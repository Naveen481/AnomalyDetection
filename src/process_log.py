'''
This script analyzes the given events of a social network and produces the anomalies. 
'''
import json
import networkx as nx
import math
from datetime import datetime

def build_initial_network(events, friendsList, purchases):
    """
        Used to build friends list and purchase list from batch_log
    """
    temp = 0
    total = len(events) # used for printing % completed in the console
    for i,s in enumerate(events):
        try:
            st = json.loads(s)
            if(temp != i*100/total):    
                print str(i*100/total)+ "%",    # Printing % completed in the console
                temp = i*100/total
            if(st["event_type"] == "purchase"):
                tempDict = dict()
                tempDict["id"] = st["id"]
                tempDict["time"] = st["timestamp"]
                tempDict["amount"] = float(st["amount"])
                purchases.append(tempDict)  # In the event of a purchase, updating the purchase list
            elif(st["event_type"] == "befriend"):
                if st["id1"] not in friendsList.keys():
                    friendsList[st['id1']] = []
                if st["id2"] not in friendsList.keys():
                    friendsList[st['id2']] = []
                friendsList[st['id1']].append(st['id2'])
                friendsList[st['id2']].append(st['id1'])
            elif (st["event_type"] == "unfriend"):
                if st['id1'] in friendsList.keys() and st['id2'] in friendsList.keys():
                    friendsList[st['id1']].remove(st['id2'])
                    friendsList[st['id2']].remove(st['id1'])
        except:
            pass
    return friendsList, purchases
    

def standardDev(mean, transactions):
    """
        Given the mean and list of transactions, this function returns the standard deviation
    """
    sum_squares = 0
    for i in transactions:
        sum_squares += (i - mean) ** 2
    sd = math.sqrt((sum_squares)/len(transactions))
    return sd
    

def find_mean_sd(friends, T, purchases):
    """
        This function finds the mean and standard deviation given the purchase list and number of transactions to
        consider
    """
    count = 0
    pos = -1
    trans = []
    while(count <= T and pos*-1 <= len(purchases)):
        if purchases[pos]["id"] in friends:
            count += 1
            trans.append(purchases[pos]["amount"])
        pos -= 1
    mean = sum(trans)/len(trans)
    sd = standardDev(mean, trans)
    return mean, sd

def build_entire_network(degree, network):
    """
        Used to build friends in the social network upto the given degree 
    """
    count = 1
    while count < degree:
        tempDict = dict()
        network[count] = dict()
        #total = len(network[0].keys())
        for user in network[0].keys():
            tempDict[user] = []
            for friend in network[count-1][user]:
                for i in network[0][friend]:
                    if count >= 2:
                        if i != user and i not in network[count-2][user]:
                            tempDict[user].append(i)
                    else:
                        if i != user:
                            tempDict[user].append(i)
        network[count] = tempDict
        count += 1
    return network
    
def update_network(event, network, degree):
    """
        Given an event it updates the user connections accordingly
    """
    friendsList = network[0]
    # Updating degree 1 friends
    try:
        if(event["event_type"] == "befreind"):
            if event["id1"] not in friendsList.keys():
                friendsList[event['id1']] = []
            if event["id2"] not in friendsList.keys():
                friendsList[event['id2']] = []
            friendsList[event['id1']].append(event['id2'])
            friendsList[event['id2']].append(event['id1'])
        elif (event["event_type"] == "unfriend"):
            if event['id1'] in friendsList.keys() and event['id2'] in friendsList.keys():
                friendsList[event['id1']].remove(event['id2'])
                friendsList[event['id2']].remove(event['id1'])
    except:
        pass
    network[0] = friendsList
    #Updating remaining degree friends
    users_to_update = []
    for i in range(degree-1):
        users_to_update.extend(network[i][event["id1"]])
        users_to_update.extend(network[i][event["id2"]])
    users_to_update = list(set(users_to_update))
    count = 1
    while count < degree:
        for user in users_to_update:
            tempList = []
            for friend in network[count-1][user]:
                for i in network[0][friend]:
                    if count >= 2:
                        if i != user and i not in network[count-2][user]:
                            tempList.append(i)
                    else:
                        if i!= user:
                            tempList.append(i)
            network[count][user] = tempList
        count += 1
    return network
  
start = datetime.now()
#Reading the input file
file = open("log_input/batch_log.json","r")
firstLine = json.loads(file.readline())     #Converts the string to dictionary
D = int(firstLine["D"])     #Degree 
T = int(firstLine["T"])     # Number of transactions to be considered
friendsList = dict()
events = file.readlines()
print "Building network..."
purchases = []
# Structure of network is such that network[0] is a dictionary that holds the list of users that are at a distance of 1
# to a specific user. network[1] contains list of users that are at a distance of 2 to a specific user and so on.. The
# size of network is equal to the degree
network = [[] for i in range(D)]
network[0], purchases = build_initial_network(events, friendsList, purchases)  #Building the network of users with  degree 1
print "100%"
file.close()
deg1 = datetime.now()
network = build_entire_network(D, network)  # Building the entire network
deg2 = datetime.now()
print "Took",deg2-deg1,"to build initial network"    # Used to print the time taken to build the initial network

file = open("log_input/stream_log.json","r")

outputFile = open("log_output/flagged_purchases.json","w")    #Output file
print "\nReading streamlined events...\n"
eventCount = 0
while True:
    temp = file.readline()  #use readline() to read lines as they update so as to mimic the streaming API
    if temp != "" and temp != "\n":
        eventCount += 1
        print "Processing event:",eventCount
        try:
            event = json.loads(temp)
            if(event["event_type"] == "purchase"):
                tempDict = dict()
                tempDict["id"] = int(event["id"])
                tempDict["time"] = str(event["timestamp"])
                tempDict["amount"] = float(event["amount"])
                friends_in_degree = []
                for i in range(D):
                    friends_in_degree.extend(network[i][event["id"]])
                friends_in_degree = list(set(friends_in_degree)) #List of all the users that are within the degree to that user
                if len(friends_in_degree) != 0:
                    mean, sd = find_mean_sd(friends_in_degree, T, purchases)
                    if(float(event["amount"]) > (mean+3*sd)):
                        outputFile.write('{"event_type":"purchase", "timestamp":"')
                        outputFile.write(str(event["timestamp"])+'", "id": "')
                        outputFile.write(str(event["id"])+'", "amount":"')
                        outputFile.write(event["amount"]+'", "mean": "')
                        outputFile.write(str(format("{:.2f}".format(mean)))+'", "sd": "')
                        outputFile.write(str(format("{:.2f}".format(sd)))+'"}\n')
                        print "\tAnomaly detected"
                purchases.append(tempDict)
            else:
                network = update_network(event, network, D)
        except:
            pass
    else:
        break

file.close()
outputFile.close()
deg3 = datetime.now()
print "Completed in ", deg3-deg1, "seconds"



print 'Do you want to visualize the network?\n\tType "Yes" or "No"\nPrinting a graph may take upto', len(network[0].keys())*0.7, "seconds"
choice = raw_input()
if choice == "Yes":
    G = nx.Graph()
    G.add_nodes_from(network[0].keys())
    for user in network[0].keys():
        for friend in network[0][user]:
            G.add_edge(user, friend)
    print "Drawing.."
    nx.draw(G)
else:
    pass

print "Graph drawn in", datetime.now() - deg3, "seconds"