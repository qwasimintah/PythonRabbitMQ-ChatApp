#!/usr/bin/env python3
import pika
import sys
import threading
import signal
import os

lock = threading.Lock()

class ChatClient(object):
    """docstring for ChatClient"""
    def __init__(self):
        self.name=""
        self.id =-1
        self.group=-1
        self.exchange ="CHATSERVER"
        self.queue_name=""
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.channel = self.connection.channel()
        self.msg_counter =1
        self.registered = False


    def callback(self,ch, method, properties, body):
        """
        Callback Function for the consume method
        """
        
        message = (body.decode("utf-8")).split(":")
        msg_type = message[0]
        group = message[1]
        id = message[2]
        content= message[3]

        if msg_type=="ACCEPTED" and self.registered == False:
            if self.id == -1 and content==self.queue_name:
                self.id = int(id)
                self.registered = True
                print("[INFO] Login Success!!!!")
                print("===============================================================")

        elif msg_type == "ACCEPTED" and self.registered == True:
            if content == self.id:
                self.id = int(id)
                self.msg_counter = 0 
        else:
            if int(group) == self.group and int(id) == self.id:
                pass
            else:
                #print(content)
                print(self.reformat_message(message))


    def reformat_message(self,message):
        msg =''
        if len(message) > 3:
            for i, mes in enumerate(message):
                if i > 3:
                    msg+=""+mes;
            return msg

        else:
            return message[3]


    def start_consumme(self):
        try:
            global lock
            while True:
                with lock:
                    self.connection.process_data_events()
        except Exception as e:
            pass


    def connect_to_server(self):
        self.channel.exchange_declare(exchange=self.exchange,exchange_type='direct')
        result = self.channel.queue_declare(exclusive=True)
        self.queue_name = result.method.queue

    def start_consume(self):
        routing_key= "group_" +str(self.group)
        self.channel.queue_bind(exchange=self.exchange,queue=self.queue_name, routing_key=routing_key)
        self.channel.basic_consume(self.callback,queue=self.queue_name,no_ack=True)
        try:
            t = threading.Thread(target=self.start_consumme)
            t.start()
        except Exception as e:
            pass

    def validate_join_msg(self,msg):
        msg_format = msg.split(" ")

        if len(msg_format) != 3:
            return False
        try:
            int(msg_format[1])
            return True
        except Exception as e:
            return False



        

    def run(self):
        while True:
            msg = input("")
            msg = msg.lower()

            if "JOINGROUP".lower() in msg:
                if self.registered == False:
                    if self.validate_join_msg(msg):
                        msg_format = msg.split(" ") 
                        group_id = msg_format[1]
                        self.name = msg_format[2]
                        self.group = int(group_id)
                        send_message = "JOIN:"+ group_id + ":"+ self.queue_name + ":"+ self.name
                        self.channel.basic_publish(exchange=self.exchange,routing_key='servers',body=send_message)
                        self.start_consume()
                    else:
                        print("Wrong Usage of JOINGROUP. Usage: JOINGROUP <#group> <username>")
                else:
                    print("[INFO] You are currently registered.")
                       
            elif "LEAVEGROUP".lower() in msg:
                try:
                    if self.group!=-1 and self.id!= -1:
                        send_message = "LEAVE:" + str(self.group) +":"+ str(self.id) 
                        self.channel.basic_publish(exchange=self.exchange,routing_key='servers',body=send_message)
                        self.connection.close()
                    else:
                        print("[INFO] You are not registered.")
                except Exception as e:
                    pass


            else:
                if self.registered == True:
                    ord_message = "@"+self.name + "_"+ str(self.id)+" : " + msg
                    to_send = 'OTHER:'+ str(self.group)+":"+ str(self.id)+":"+ str(self.msg_counter)+ ":"+ord_message
                    global lock
                    with lock:
                        self.channel.basic_publish(exchange=self.exchange,routing_key='servers',body=to_send)
                    self.msg_counter += 1
                else:
                    print("[INFO] You are not registered.")

def signal_handler(signal, frame):
    os.system('kill -9 %s' %os.getpid())

def welcome():
    print("#######################################################################")
    print("##                                                                   ##")
    print("########## Welcome to Challey Chat Service ############################")             
    print("[INFO] To start chatting, join a group. JOINGROUP <#group> <username> ")
    print("######################################################################")
    print("")            



def main():
    welcome()
    client =ChatClient()
    client.connect_to_server()
    client.run()

if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal_handler)
    main()    
    signal.pause()

