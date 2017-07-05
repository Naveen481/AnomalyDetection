'''
This script analyzes the given events of a social network and produces the anomalies. 
'''
import json
import math

def build_network(events, friendsList, purchases):
    """
        Used to build initial network and purchase list
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


def update_network(event, friendsList):
    """
        Given an event it updates the network
    """
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
    return friendsList
    

def find_friends(id, degree, friendsList):
    """
        Given an user id and a degree, this function gives the friends of that user within that degree.
        The returned list does not include the user
    """
    friends = set(friendsList[id])
    count = 0
    tempFriends = friends
    change = True
    while count <= degree and change:
        change = False
        temp = set()
        for friend in tempFriends:
            temp.update(friendsList[friend])
        tempFriends = temp
        if len(tempFriends - friends) != 0:
            friends.update(temp)
            change = True
        count += 1
    friends.discard(id)
    return list(friends)
    

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
        This function finds the mean and standard deviation from the purchases list and number of transactions to consider
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
    
    
#Reading the input file
file = open("log_input/batch_log.json","r")
firstLine = json.loads(file.readline())     #Converts the string to dictionary
D = int(firstLine["D"])
T = int(firstLine["T"])
friendsList = dict()
events = file.readlines()
print "Building network..."
purchases = []
friendsList, purchases = build_network(events, friendsList, purchases)  #Building the initial network
print "100%"
file.close()

#Reading the stream_log.json
file = open("log_input/stream_log.json","r")

outputFile = open("log_output/flagged_purchases.json","w")    #Output file
print "\nReading streamlined events...\n"
eventCount = 0
while True:
    temp = file.readline()
    if temp != "":
        eventCount += 1
        print "Processing event:",eventCount
        try:
            event = json.loads(temp)
            if(event["event_type"] == "purchase"):
                tempDict = dict()
                tempDict["id"] = int(event["id"])
                tempDict["time"] = str(event["timestamp"])
                tempDict["amount"] = float(event["amount"])
                friends_in_degree = find_friends(event["id"], D, friendsList)
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
                friendsList = update_network(event, friendsList)
        except Exception,e:
            print e
    else:
        break
    
file.close()
outputFile.close()