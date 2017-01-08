from datetime import datetime, timedelta
import pandas as pd

print "read user engagement ... "
with open('/Users/yueliu/Desktop/user_engagement.csv') as f:
    lines = f.readlines()
    data = lines[0].split('\r')


user2dict = {}
user2label = {}

print "get counts ..."
# main method
def windowCounts(data, w, skip=1):
    """this function is to help count occurance of objects in a time window. i.e. say user Danny has several log in records,
    we want to find how many logins he made during a time window
    
    input: 
    data: log in information, list of strings
    w: window size, int
    skip: the number of rows to skip when processing the data
    
    idea: for each timestamp of each user, find how many logins she made during the last w days and next w days
    """
    user2dict = {}
    for i in range(skip,len(data)):
        info = data[i].split(',')
    #     get time and user id
        time = datetime.strptime(info[0].split(' ')[0], '%m/%d/%y')
        user = info[1]
    #     if user not seen, initialize a [1,1] tuple, map the user to time-count pair
        if user not in user2dict:
            date2list = {}
            date2list[time] = [1,1]
            user2dict[user] = date2list
        else:
    #         get the time-count pairs
            date2list = user2dict[user]
    #         if the time is not seen, initialize a time-count pair
            if time not in date2list:
                date2list[time] = [1,1]
    #       check last 7 days and next 7 days
            for d in range(1,w+1):
                delta = timedelta(days = d)
    #for next 7 days (call them rightTime): if we have seen any day of them, 
    #increment the left count of rightTime and rightCount of currentTime
                rightTime = time + delta
                if rightTime in date2list:
                    rightList = date2list[rightTime]
                    rightList[0] = rightList[0]+1
                    date2list[rightTime] = rightList

                    List = date2list[time]
                    List[1] = List[1]+1
                    date2list[time] = List
    #for last 7 days (call them leftTime): if we have seen any day of them, 
    #increment the right count of leftTime and leftCount of currentTime            
                leftTime = time - delta
                if leftTime in date2list:
                    leftList = date2list[leftTime]
                    leftList[1] = leftList[1]+1
                    date2list[leftTime] = leftList

                    List = date2list[time]
                    List[0] = List[0]+1
                    date2list[time] = List
    #         after updating, map user to its corresponding time-count pairs
    user2dict[user] = date2list
    return user2dict

user2dict = windowCounts(data, 6, 1)

for user in user2dict.keys():
    d = user2dict[user]
    user_int = int(user)
    user2label[user_int] = 'not adopted'
    for freqList in d.values():
        if max(freqList) >= 3:
            user2label[user_int] = 'adopted'
            break

print "labeling..."
# get all the labels based on count information
user_info = pd.read_excel('/Users/yueliu/Desktop/users.xls')

labels = []
for id in user_info['object_id']:
    if id in user2label:
        labels.append(user2label[id])
    else:
        labels.append(None)

user_info['label'] = labels
user_info.to_csv('labeledData.csv')

print "done"