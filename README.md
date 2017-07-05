# Challenge Summary

Imagine you're at an e-commerce company, Market-ter, that also has a social network. In addition to shopping, users can see which items their friends are buying, and are influenced by the purchases within their network. 

Your challenge is to build a real-time platform to analyze purchases within a social network of users, and detect any behavior that is far from the average within that social network.

# Result

The challenge was implemented using Python as I think it is the most preferred language to work with large data. The program was executed in 1.51 minutes for sample_dataset provided with the challenge. All the other test cases were successfully completed. The program was also checked against other test cases which I thought would be the edge cases.

# Additional features

A network graph was implemented. The initial aim was to implement a 3d graph using ployly, but due to time constraints and other deadlines, I implemented only a 2d graph with networkx. This is only an idea to visualize the network as it grows, but not fully implemented as it was initially thought.

# Required packages
Most of the program was implemented using inbuilt functions, but some other basic packages were also used.
1. json
2. networkx
3. math
4. datetime

# Algorithm

1. Inital friends list was built from batch_log.json
2. Then friends who are at a distance of degree two are found and then stored. This is done upto given degree.
3. While streaming from stream_log.json, if an event is a purchase, then the friends of the user within the degree are found and the purchases made by them are only considered to build the mean and standard deviation.
4. Then anomalies are detected by comparing them.
5. Incase the event is a "befriend" or "unfriend", the friends of both the ids within the degree are considered and their network is updated.
