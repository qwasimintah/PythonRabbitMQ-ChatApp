#!/usr/bin/env python3
import pika
import threading
import argparse
import signal
import sys
import os
import datetime
import time
import json

from datetime import timedelta
global pub_loc
lock = threading.Lock()
lock2 = threading.Lock()
pub_loc = threading.Lock()

class ServerNode(threading.Thread):
    """docstring for ServerNode"""
    def __init__(self, id,num):
        threading.Thread.__init__(self)
        self.id = id
        self.send_queue = ""
        self.consume_queue = ""
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.channel = self.connection.channel()
        self.service_ch = self.connection.channel()
        self.alive_connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.alive_channel = self.alive_connection.channel()
        self.num = num
        self.exchange ="CHATSERVER"
        self.service_queue=""
        self.clients_index=id
        self.registered_clients = {}
        self.active_servers ={}
        self.alive_msg =0
        self.reconstructed = []
        self.active = True


    def get_user_name(self, group, id):

        return self.registered_clients[group][id]["name"];

    def alive_callback(self,ch, method, properties, body):
        orig_msg = body.decode("utf-8")
        message = str(orig_msg).split(":")
        # first check message group
        msg_type = message[0]

        if msg_type =="ALIVE":
            server_id = message[1]
            if int(server_id) != self.id:
                self.update_server_status(server_id, True)
            return

    def get_next_in_ring(self,key):

        for x in range(key +1 , 1000):
            if x not in self.reconstructed:
                return x
            

    def display_ring(self):
        if self.active:
            if self.id == 1:
                print(str(self.id)+ " ===> "+ str(self.get_next_in_ring(self.id)), end = " ")
            elif self.id == self.num:
                print(' ===> ' + str(self.get_next_in_ring(0)), end = " ")
            else:
                print(str(' ===> '+ str(self.get_next_in_ring(self.id))), end = " ")



    def show_client_status(self):
        result = ""
        print("######## STATE OF SERVER "+ str(self.id) + " Status: ", self.active, "########")
        if self.active:
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
                    self.update_left(group, client_id)
                    name = self.get_user_name(group, client_id)
                    msg= "LEAVE:"+ str(group)+":"+ str(client_id) +":" + name + " left"
                    ch.basic_publish(exchange=self.exchange, routing_key=self.get_routing(group), body=msg)
                else:
                    mid = int(message[3])
                    self.push_messages(group,int_id, mid ,orig_msg)
                    to_send = self.messages_to_send(group,int_id)
                    for mess in to_send:
                        with pub_loc: 
                            ch.basic_publish(exchange=self.exchange,routing_key=self.get_routing(group),body=mess["message"])
                            self.update_message_sent(group,int_id, mess["mid"])

            else:
                server_id = self.get_server(int_id)
                next_server = self.get_next_server() 
                if server_id not in self.reconstructed:                    
                    with pub_loc:
                        ch.basic_publish(exchange='',routing_key =self.send_queue ,body=orig_msg,properties=pika.BasicProperties(content_type='text/plain',delivery_mode=1))
                else:
                    if next_server == server_id:
                        assigned_id = self.clients_index
                        self.clients_index += self.num
                        accept_msg ="ACCEPTED:" + group +":"+ str(assigned_id) + ":" + id;
                        with pub_loc:
                            ch.basic_publish(exchange=self.exchange, routing_key=self.get_routing(group), body=accept_msg)
                        self.register_client(group, int_id, "Client with id" + id)
                        with pub_loc:
                            ch.basic_publish(exchange=self.exchange, routing_key=self.get_routing(group), body=orig_msg)
                    #server node dead is an intermediary node
                    else:
                        with pub_loc:
                            ch.basic_publish(exchange='',routing_key =self.send_queue ,body=orig_msg,properties=pika.BasicProperties(content_type='text/plain',delivery_mode=1))


    def reconstruct_network(self):
        next_id = self.get_next_server()
        self.send_queue = 'queue'+str(next_id)
        print("[INFO] Detected that Server ", next_id, "has failed. Reconstructing Network")
        self.reconstructed.append(next_id)



    def get_next_server(self):

        if self.id == self.num:
            return 1
        return self.id+1

    def start_consume(self):
        try:
            global lock
            while True:
                with lock:
                    self.connection.process_data_events()
        except Exception as e:
            pass


    def start_consume2(self):
        try:
            global lock2
            while True:
                with pub_loc:
                    self.alive_connection.process_data_events()
        except Exception as e:
            pass
        


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

    def update_server_status(self, server_id, status):
        if server_id not in self.active_servers:
            self.active_servers[server_id]={}
        self.active_servers[server_id] = {"status": status, "timestamp": datetime.datetime.now()}


    def send_alive_message(self):
        orig_msg = "ALIVE:"+str(self.id)+": : "
        
        if self.alive_msg > 1:
            if self.id == self.num:
                next_server = 1
            else:
                next_server = self.id + 1 
            alive = self.is_server_alive(str(next_server))
            if alive:
                with pub_loc:
                    self.alive_channel.basic_publish(exchange='alive',routing_key ='online' ,body=orig_msg)
            else:
                if next_server not in self.reconstructed:
                    self.reconstruct_network()
                
                with pub_loc:
                    self.alive_channel.basic_publish(exchange='alive',routing_key ='online' ,body=orig_msg)
        else:
            with pub_loc:
                self.alive_channel.basic_publish(exchange='alive',routing_key ='online' ,body=orig_msg)

        self.alive_msg+=1

     
    def update_left(self,group_id, client_id):
        self.registered_clients[group_id][client_id]["status"] = False

    def is_server_alive_it(self,server_id):
        try:
            last_date = self.active_servers[server_id]["timestamp"]

            delta = datetime.datetime.now() - last_date
            days, seconds = divmod(delta.total_seconds(), 60)
            if seconds > 15:
                return False
            return True
        except Exception as e:
            print   ("error" + str(e))
            return False

    def is_server_alife(self,server_id, date):
        try:
            last_date = self.active_servers[server_id]["timestamp"]

            delta = date - last_date
            days, seconds = divmod(delta.total_seconds(), 60)
    
            return seconds
        except Exception as e:
            print   ("error" + str(e))
            return False

    def is_server_alive(self, server_id):
        seconds=[]
        date = datetime.datetime.now()
        for server in self.active_servers:
            seconds.append(self.is_server_alife(server,date))

        ref = self.is_server_alife(server_id, date)
        dead = max(seconds)
        #print("@"+str(self.id), dead)
        if dead >= 15 and dead == ref:
            return False

        else:
            return True



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



    def get_routing(self,group):

        return 'group_'+str(group)

    def get_server(self, id):
        if id % self.num ==0:
            return self.num
        else:
            return id%self.num

    def server_callback(self,ch, method, properties, body):

        orig_msg = body.decode("utf-8")
        message = str(orig_msg).split(":")
        # first check message group
        msg_type = message[0]

        if msg_type =="ALIVE":
            server_id = message[1]
            if int(server_id) != self.id:
                self.update_server_status(server_id, True)
                with pub_loc:
                    ch.basic_publish(exchange='',routing_key =self.send_queue ,body=orig_msg)
            return

        group = message[1]
        id = message[2]
        server_id = self.get_send_node()
        self.update_server_status(server_id, True)

        int_id = int(id)
        if self.get_server(int_id) == int(self.id):
            if msg_type =="LEAVE":                      
                id = message[2]
                client_id = int(str(id))
                name = self.get_user_name(group, client_id)
                self.update_left(group, client_id)
                msg= "LEAVE:"+ str(group)+":"+ str(client_id) +":" + name + " left"
                ch.basic_publish(exchange=self.exchange, routing_key=self.get_routing(group), body=msg)

                
            else:
                mid= int(message[3])
                self.push_messages(group,int_id, mid, orig_msg)
                to_send = self.messages_to_send(group,int_id)
                for mess in to_send: 
                    
                    with pub_loc:
                        ch.basic_publish(exchange=self.exchange,routing_key=self.get_routing(group),body=mess["message"])
                        self.update_message_sent(group,int_id, mess["mid"])
        else:
            server_id = self.get_server(int_id)
            if self.id == self.num:
                next_server = 1
            else:
                next_server = self.id + 1 
            if server_id not in self.reconstructed:                
                with pub_loc:
                    ch.basic_publish(exchange='',routing_key =self.send_queue ,body=orig_msg)
            else:
                if next_server == server_id:
                    assigned_id = self.clients_index
                    self.clients_index += self.num
                    accept_msg ="ACCEPTED:" + group +":"+ str(assigned_id) + ":" + id;
                    
                    with pub_loc:
                        ch.basic_publish(exchange=self.exchange, routing_key=self.get_routing(group), body=accept_msg)
                    self.reconstruct_network()
                    self.register_client(group, int_id, "Client with id" + id)
                    with pub_loc:
                        ch.basic_publish(exchange=self.exchange, routing_key=self.get_routing(group), body=orig_msg)
                #server node dead is an intermediary node
                else:
                    with pub_loc:
                        ch.basic_publish(exchange='',routing_key =self.send_queue ,body=orig_msg,properties=pika.BasicProperties(content_type='text/plain',delivery_mode=1))


    def get_send_node(self):

        if self.id == 1:
            return str(self.num)
        else:
            return str(self.id-1)

    def set_ring_queues(self):
        if self.id == 1:
            self.send_queue = "queue"+str(self.id)
            self.consume_queue = "queue"+str(N)

        else:
            self.send_queue = "queue"+str(self.id)
            self.consume_queue = "queue"+str(self.id-1)

    def set_service_queue(self):
        self.channel.exchange_declare(exchange=self.exchange,exchange_type='direct')
        result = self.channel.queue_declare(queue="service_queue")
        self.service_queue = "service_queue"
        self.channel.queue_bind(exchange=self.exchange,queue=self.service_queue, routing_key="servers")
        self.channel.basic_consume(self.service_callback,queue=self.service_queue,no_ack=True)
        t = threading.Thread(target=self.start_consume)
        t.start()

    def set_server_queue(self):
        self.service_ch.queue_declare(queue=self.consume_queue)
        self.service_ch.queue_declare(queue=self.send_queue)
        self.service_ch.basic_consume(self.server_callback,queue=self.consume_queue,no_ack=True)

        self.alive_channel.exchange_declare(exchange='alive',exchange_type='direct')
        result = self.alive_channel.queue_declare(exclusive=True)
        self.alive_channel.queue_bind(exchange='alive',queue=result.method.queue,routing_key='online')
        self.alive_channel.basic_consume(self.alive_callback,queue=result.method.queue,no_ack=True)
        t1 = threading.Thread(target=self.start_consume2)
        t1.start()


    def kill_server(self):
        self.active=False
        self.connection.close()
        self.alive_connection.close()

    def send_live(self):
        try:
            self.send_alive_message()
            threading.Timer(10, self.send_live).start()            
        except Exception as e:
            pass
        

    def run(self):
        try:
            self.set_ring_queues()
            t = threading.Thread(target=self.set_service_queue)
            t.start()
            self.set_server_queue()
            self.send_live()
        except Exception as e:
            pass

        


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
        print("Usage: python3 server_ring_fail.py -n <# of servers>")
        sys.exit()
 
    N = args.n[0]

    welcome(N)
    server_nodes =[]
    for i in range(1, N+1):
        node = ServerNode(i, N)
        node.start()
        server_nodes.append(node)
    

    signal.signal(signal.SIGINT, signal_handler)
    


    while True:
        print("[INFO] SERVER READY ................")
        a = input("")
        msg = a.lower()
        if "KILLNODE".lower() in msg:
            try:
                thread_id = int(msg.split(" ")[1]) -1
                node = server_nodes[thread_id]
                node.kill_server()
                print("Server "+str(thread_id+1)+ " killed")
            except Exception as e:
                pass
                        
                   
        elif "STATE".lower() in msg:
            print("############### SERVER STATUS #################")
            for nodes in server_nodes:
                nodes.show_client_status()
            print("==============================================")
            print("\n") 
            print("################ RING VIEW #####################")
            for nodes in server_nodes:
                nodes.display_ring()

            print("\n==============================================")


        else:
            print("Incorrect Usage: Use Killnode <#id> or state")
    signal.pause()




    





