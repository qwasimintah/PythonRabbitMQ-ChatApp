#!/usr/bin/env python3
import pika
import threading
import argparse
import signal
import sys
import os

global lock
global pub_lock
lock = threading.Lock()
pub_lock = threading.Lock()

class ServerNode(threading.Thread):
    """docstring for ServerNode"""
    def __init__(self, id,num):
        threading.Thread.__init__(self)
        self.id = id
        self.send_queue = ""
        self.consume_queue = ""
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.channel = self.connection.channel()
        self.num = num
        self.exchange ="CHATSERVER"
        self.service_queue=""
        self.clients_index=id
        self.registered_clients = {}


    def start_consume(self):
        try:
            while True:
                with lock:
                    self.connection.process_data_events()
        except Exception as e:
            pass



    def get_server(self, id):
        if id % self.num ==0:
            return self.num
        else:
            return id%self.num


    def get_user_name(self, group, id):

        return self.registered_clients[group][id]["name"];

    def service_callback(self,ch, method, properties, body):
        #print(str(body))
        orig_msg = body.decode("utf-8")
        message = str(orig_msg).split(":")
        # first check message group
        msg_type = str(message[0])
        group = str(message[1])
        
        if msg_type=="JOIN":
            assigned_id = self.clients_index
            self.clients_index += self.num
            queue_id = message[2]
            name = message[3]
            msg = "ACCEPTED:" + group +":"+ str(assigned_id) + ":" + queue_id 
            with pub_lock:
                ch.basic_publish(exchange=self.exchange,routing_key='group_'+str(group),body=msg)
            #print(" [x] Sent %r" % msg)
            self.register_client(group, assigned_id, name)

        else:
            id = message[2]
            int_id = int(str(id))
            
            if self.get_server(int_id) == int(self.id):

                if msg_type =="LEAVE":
                    id = message[2]
                    client_id = int(str(id))
                    #mid = int(message[3])
                    name = self.get_user_name(group, client_id)
                    msg= "LEAVE:"+ str(group)+":"+ str(client_id) +":" + name + " left"
                    self.update_left(group,client_id)
                    #print(msg)
                    with pub_lock:
                        ch.basic_publish(exchange=self.exchange, routing_key=self.get_routing(group), body=msg)
                else:
                    mid = int(message[3])
                    self.push_messages(group,int_id, mid ,orig_msg)
                    to_send = self.messages_to_send(group,int_id)
                    for mess in to_send:
                        with pub_lock: 
                            ch.basic_publish(exchange=self.exchange,routing_key=self.get_routing(group),body=mess["message"])
                        self.update_message_sent(group,int_id, mess["mid"])
                        #print("matched published message", orig_msg)
                        #print(self.registered_clients)
            else:
                with pub_lock:
                    ch.basic_publish(exchange='',routing_key =self.send_queue ,body=orig_msg)
                #print("didnt match dispatch", orig_msg)


    def register_client(self,group_id, client_id, client_name):

        if group_id in self.registered_clients:
            if client_id not in self.registered_clients[group_id]:
                self.registered_clients[group_id][client_id]={}
                self.registered_clients[group_id][client_id]["name"]=client_name
                self.registered_clients[group_id][client_id]["messages"]=[]
                self.registered_clients[group_id][client_id]["status"]=True
        else:
            self.registered_clients[group_id]={}

            if client_id not in self.registered_clients[group_id]:
                self.registered_clients[group_id][client_id]={}
                self.registered_clients[group_id][client_id]["name"]=client_name
                self.registered_clients[group_id][client_id]["messages"]=[]
                self.registered_clients[group_id][client_id]["status"]=True

        return True

    def update_left(self,group_id, client_id):
        self.registered_clients[group_id][client_id]["status"] = False

    def push_messages(self,group_id, client_id, mid, message):

        mess = { "mid" : mid, "message": message, "sent": False}

        self.registered_clients[group_id][client_id]["messages"].append(mess)


    def update_message_sent(self,group_id, client_id, mid):

        all_messages = self.registered_clients[group_id][client_id]["messages"]
        index = None
        for i, mess in enumerate(all_messages):
            if mess["mid"] == mid:
                index = i

        if index !=None:
            self.registered_clients[group_id][client_id]["messages"][index]["sent"] = True
            

    def messages_to_send(self,group_id, client_id):

        client_messages = self.registered_clients[group_id][client_id]["messages"]

        unsent = self.get_all_unsent_sorted(client_messages)

        to_send = self.get_ready_to_send(unsent, client_messages)

        return to_send
        


    def get_all_unsent_sorted(self,all_messages):

        unsent =[]

        for mess in all_messages:
            if mess["sent"]==False:
                unsent.append(mess)
        
        # sort based on message id
        unsent_sorted = sorted(unsent, key=lambda k: k['mid'])

        return unsent_sorted


    def get_last_sent(self,all_messages):
        listsorted = sorted(all_messages, key=lambda k: k['mid'])

        for mes in listsorted:
            if mes["sent"]==False:
                return mes["mid"] - 1
        return 0

    def get_ready_to_send(self,unsent_sorted, all_messages):
        last_sent = self.get_last_sent(all_messages)
        to_send =[]
        for i, message in enumerate(unsent_sorted):
            if message["mid"]== last_sent +1:
                to_send.append(message)
                last_sent = message['mid']
            else:
                break
        return to_send

    def show_server_messages(self):
        if len(self.registered_clients) ==0:
            print("[INFO] No results found")
        else:
            print(self.registered_clients)


    def show_client_status(self):
        result = ""
        print("######## STATE OF SERVER "+ str(self.id))
        for group, content in self.registered_clients.items():
            result+="\n Group "+ str(group)
            result+="\n"
            for client, data in content.items():
                if data["status"]:
                    status = 'active'
                else:
                    status = 'left'
                result+='ID: ' + str(client) +' Name: ' + str(data["name"]) + " Status: " + status
                result+="\n"

        print(result)


    def get_routing(self,group):

        return 'group_'+str(group)

    def server_callback(self,ch, method, properties, body):

        orig_msg = body.decode("utf-8")
        message = str(orig_msg).split(":")
        # first check message group
        msg_type = message[0]
        group = message[1]
        id = message[2]
        

        int_id = int(id)

        if self.get_server(int_id)== int(self.id):
            if msg_type =="LEAVE":                      
                id = message[2]
                client_id = int(str(id))
                name = self.get_user_name(group, client_id)
                msg= "LEAVE:"+ str(group)+":"+ str(client_id) +":" + name + " left"
                self.update_left(group, client_id)
                #print(msg)
                with pub_lock:
                    ch.basic_publish(exchange=self.exchange, routing_key=self.get_routing(group), body=msg)

                
            else:
                mid= int(message[3])
                self.push_messages(group,int_id, mid, orig_msg)
                to_send = self.messages_to_send(group,int_id)
                for mess in to_send:
                    with pub_lock:
                        ch.basic_publish(exchange=self.exchange,routing_key=self.get_routing(group),body=mess["message"])
                    self.update_message_sent(group,int_id, mess["mid"])
                    #print(self.registered_clients)
                    #print("matched published message", orig_msg)
        else:
            with pub_lock:
                ch.basic_publish(exchange='',routing_key =self.send_queue ,body=orig_msg)
            #print("didnt match dispatch", orig_msg)

    def set_ring_queues(self):
        if self.id == 1:
            self.send_queue = "queue"+str(self.id)
            self.consume_queue = "queue"+str(N)

        else:
            self.send_queue = "queue"+str(self.id)
            self.consume_queue = "queue"+str(self.id-1)

        #print(self.send_queue, self.consume_queue)

    def set_service_queue(self):
        self.channel.exchange_declare(exchange=self.exchange,exchange_type='direct')
        result = self.channel.queue_declare(queue="service_queue")
        self.service_queue = "service_queue"
        self.channel.queue_bind(exchange=self.exchange,queue=self.service_queue, routing_key="servers")
        self.channel.basic_consume(self.service_callback,queue=self.service_queue,no_ack=True)
        #self.channel.start_consuming()
        self.start_consume()

    def set_server_queue(self):
        self.channel.queue_declare(queue=self.consume_queue)
        self.channel.basic_consume(self.server_callback,queue=self.consume_queue,no_ack=True)
        #self.channel.start_consuming()


    def run(self):
        self.set_ring_queues()
        #self.set_service_queue()
        t = threading.Thread(target=self.set_service_queue)
        t.start()
        self.set_server_queue()

def welcome(N):
    print("#############################################################")
    print("#                                                           #")
    print("############ Welcome ChalleyChat Server #####################")
    print("Number of Servers Running: "+str(N))



def signal_handler(signal, frame):
    os.system('kill -9 %s' %os.getpid())



if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Process number of nodes')
    parser.add_argument('-n', metavar='', type=int, nargs='+',
                        help='number of nodes in server ring')


    args = parser.parse_args()
    if args.n == None:
        print("Usage: python3 server_ring_order.py -n <# of servers>")
        sys.exit()

    N = args.n[0]

    server_nodes =[]
    for i in range(1, N+1):
        node = ServerNode(i, N)
        node.start()
        server_nodes.append(node)
    welcome(N)

    signal.signal(signal.SIGINT, signal_handler)
    #signalshow.pause()




    while True:
        print("[INFO] SERVER IS READY....")
        a = input("")
        msg = a.lower()

        if "STATE".lower() in msg:
            for nodes in server_nodes:
                nodes.show_client_status()

        elif "messages".lower() in msg:
            thread_id = int(msg.split(" ")[1]) -1
            node = server_nodes[thread_id]
            node.show_server_messages()

        else:
            pass
    signal.pause()
    





