******************************************************
* Welcome to CHALEY CHAT APPLICATION  WITH RABBITMQ  *
******************************************************

###########
# AUTHORS #
###########

SOSNIN ANDREY
DJAN DENNIS MINTAH

####################
#  DEPENDENCIES   ##
####################


python3
pika: This can be installed simply by running "pip install pika"
argparse: This can be installed simply by running "pip3 install argparse"

#######
# RUN #
#######

We provide 2 version of our solution

1. VERSION 1: Core Functionality: Join, Message, Leave + Maintaing total order

To run this version

*SERVER


$ python3 server_ring_order.py -n <#number of server nodes>

$ python3 server_ring_order.py -n 4


*CLIENT

$ python3 chat_client_order.py



2. VERSION 2: Core Functionality: Join, Message, Leave + Maintaing total order + Network Failure Tolerance

To run this version

*SERVER


$ python3 server_ring_fail.py -n <#number of server nodes>

$ python3 server_ring_fail.py -n 4


*CLIENT

$ python3 chat_client_fail.py




######################
## FUNCTIONALITIES ##
#####################

+++++++++++++++++
+ JOINGROUP CHAT+
+++++++++++++++++
To start chatting a user must join the chat by using the following command

$ JOINGROUP <#GROUP NUMBER> <USERNAME>

eg

$ JOINGROUP 1 dennis

* NOTE Groups are automatically created when it doesn't exist

+++++++++++++++++++
+ MESSAGE +
+++++++++++++++++++
This feature allows one to send message to the group

eg.
$ hello

+++++++++++++++++
+ LEAVEGROUP +
+++++++++++++++++
This feature allows a user to leave a group. 

eg.
$ leavegroup


+++++++++++++++++++++
+ VIEW SERVER STATE +
+++++++++++++++++++++

This feature allows to view the current set of users registered on the server and their 
status as well as the status of the server. This functionality is only available on the server
console

eg.
$ state


+++++++++++
+ KILLNODE +
+++++++++++
This functionality is used to simulate node failures of the servers. This functionality is only available on the server console. 

$ killnode 3


+++++++++++++++++++++++++++++++++++++
+ TESTING NETWORK FAILURE TOLERANCE +
+++++++++++++++++++++++++++++++++++++
To test this on the network, First run the server
The number of servers must be greater than 3 so that after we simulate a server fail we
can still have a ring.

eg. $ python3 server_ring_fail.py -n 4

wait until the server responds with 

$ [INFO] SERVER READY

To ensure that the server is running perfectly you can view the state of the server as follows

$ state

To simulate the test run several clients (on different terminals) and register them on the network as

$ python3 chat_client_fail.py

And register them accordingly with 

$ joingroup 1 user1


You can simulate a network failure by killing one of the server nodes

On the server use the command

$ killnode 2.

This kills Server 2. Ignored the exeptions thrown as a result of the abrupt closure of the connection


After a while you should observe that a node detects and reconstruct the network.


You can view the new ring structure by the state command.

$ state


And the you can check that the network stills works by sending messages with thecurrently registered nodes.










