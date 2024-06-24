import socket
import select
import random
import hashlib
from datetime import datetime
import time
import pickle

class Message:
    def __init__(self, message, sender) -> None:
        self.message = message
        self.sender = sender
        self.hash = hashlib.sha256(message.encode()).hexdigest()


class PeerNode:
    def __init__(self, ip, port) -> None:
        self.ip = ip
        self.port = port
        self.listeningSocket = None         # Socket for listening to new connection requests
        self.seeds = []                     # Address information of connected seeds
        self.peers = []                     # Address information of connected peers
        self.seedSockets = []               # Socket objects of connected seeds
        self.peerConnections = []           # Connection objects of connected peers
        self.peerSockets = []               # Socket objects of connected peers
        self.messageList = []               # List of Messages
        self.livenessStatus = {}            # Liveness testing - Count of unanswered liveness requests
        self.sentMessageCount = 0           # Count of sent gossip messages
        self.lastGossipMessageTime = None   # Timestamp of last sent gossip message
        self.lastLivenessMessageTime = None # Timestamp of last sent liveness message
        self.socketMapping = {}             # socket to address mapping
        self.allConnections = []          
        
    def setup(self) -> None:
        self.listeningSocket =  socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listeningSocket.bind((self.ip, self.port))
        
        
    def readConfigurations(self, filePath):
        seeds = []
        try:
            with open(filePath, 'r') as file:
                for line in file:
                    line = line.strip()
                    if line:
                        seedIp, seedPort = line.split(':')
                    seeds.append((seedIp, int(seedPort)))

        except Exception as e:
            print(e)
            
        return seeds
    
    def getConnection(self, address):
        # Retrieve socket object from address
        for socket in self.socketMapping:
            if(self.socketMapping[socket] == address):
                return socket

    def establishConnections(self) -> None:
        # Connect to seed nodes
        seeds = self.readConfigurations(filePath = 'config.txt')
        
        seeds = random.sample(seeds, len(seeds))
        allPeers = []
        iterator = 0

        while(len(self.seeds) < (len(seeds) / 2 + 1) and iterator < len(seeds)):
            sock =  socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                sock.connect(seeds[iterator])
            
                peers = sock.recv(1024)
                peers = pickle.loads(peers)
                
                output = "List of Peers : " + str(peers)
                print(output)
                self.writeLog(output)
                
                allPeers = list(set(allPeers).union(peers))
                
                self.seeds.append(seeds[iterator])
                self.seedSockets.append(sock)
                
                myAddress = str((self.ip, self.port))
                sock.send(myAddress.encode())
            except Exception as e:
                print("1")
                print(e)
            iterator += 1

        # Connect to peer nodes
        allPeers = random.sample(allPeers, len(allPeers))
        iterator = 0
        
        while(len(self.peers) < 4 and iterator < len(allPeers)):
            sock =  socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                sock.connect(allPeers[iterator])
                self.peers.append(allPeers[iterator])
                self.livenessStatus[allPeers[iterator]] = 0
                self.peerSockets.append(sock)
                
                self.socketMapping[sock] = allPeers[iterator]

                myAddress = str((self.ip, self.port))
                sock.send(myAddress.encode())
            except:
                pass
            iterator += 1
            
            
    def acceptConnection(self) -> None:
        clientSocket, clientAddress = self.listeningSocket.accept()
        clientAddress = clientSocket.recv(1024).decode()
        clientAddress = eval(clientAddress)

        self.peers.append(clientAddress)
        self.livenessStatus[clientAddress] = 0
        self.peerConnections.append(clientSocket)
        self.socketMapping[clientSocket] = clientAddress
        self.allConnections.append(clientSocket)

        output = f"New connection from {clientAddress}"
        print(output)
        self.writeLog(output)
    
    
    def sendMessage(self, mes, receivers) -> None:
        try:
            for socket in receivers:
                socket.send(mes.encode())
        except:
            pass
            
    def generateGossipMessage(self):
        timestamp = datetime.now().strftime("%H.%M.%S")
        messageText = "Gossip Message " + str(self.sentMessageCount + 1)
        gossipMessage = timestamp + ":" + str((self.ip,self.port)) + ":" + messageText
        
        newMessage = Message(gossipMessage, None)
        self.messageList.append(newMessage)

        return gossipMessage
    
    def generateLivenessMessage(self):
        timestamp = datetime.now().strftime("%H.%M.%S")
        livenessMessage = "Liveness Request:" + timestamp + ":" + str((self.ip,self.port))
        return livenessMessage
    
    def removeDeadNodes(self):
        toBeRemoved = []
        
        for peer in self.livenessStatus.keys():
            if self.livenessStatus[peer] > 3:
                peerSocket = self.getConnection(peer)
                timeStamp = datetime.now().strftime("%H.%M.%S")
                
                mes = "Dead Node:"+str(peer)+":"+timeStamp+":"+str((self.ip, self.port))
                print(mes)
                self.writeLog(mes)

                self.sendMessage(mes, self.seedSockets)
                
                self.peers.remove(peer)
                toBeRemoved.append(peer)
                
                if(peerSocket in self.peerSockets):
                    self.peerSockets.remove(peerSocket)
                    
                if(peerSocket in self.peerConnections):
                    self.peerConnections.remove(peerSocket)
                    
                if(peerSocket in self.allConnections):
                    self.allConnections.remove(peerSocket)
                
                peerSocket.close()
                
        for peer in toBeRemoved:
            self.livenessStatus.pop(peer)

       
    def activate(self) -> None:
        self.listeningSocket.listen()
        self.allConnections = [self.listeningSocket] + self.seedSockets + self.peerSockets + self.peerConnections
        
        self.lastGossipMessageTime = time.time()
        self.lastLivenessMessageTime = time.time()
        
        while True:
            timeout = 1
            readable, _, _ = select.select(self.allConnections, [], [], timeout)
            
            current_time = time.time()
            
            # Generate and broadcast gossip message every 5 seconds using timer functions
            if (current_time - self.lastGossipMessageTime) >= 10 and self.sentMessageCount < 10:
                gossipMessage = self.generateGossipMessage()
                
                receivers = self.peerConnections + self.peerSockets
                self.sendMessage(gossipMessage, receivers)
                
                self.sentMessageCount += 1
                self.lastGossipMessageTime = current_time
                
                output = "Generated Gossip Message : " + gossipMessage
                print(output)
                self.writeLog(output)

            
            # Generate and broadcast liveness message every 13 seconds using timer functions
            if current_time - self.lastLivenessMessageTime >= 13:
                livenessMessage = self.generateLivenessMessage()
                receivers = self.peerConnections + self.peerSockets
                self.sendMessage(livenessMessage, receivers)
                self.lastLivenessMessageTime = current_time
                
                # Add 1 to livenessStatus of each connected peer
                self.livenessStatus = {key: value + 1 for key, value in self.livenessStatus.items()}
                
                # Remove dead nodes if found
                self.removeDeadNodes()
            
            
            for sock in readable:
                if sock == self.listeningSocket:
                    if((len(self.peerConnections) + len(self.peerSockets)) < 4):
                        # Accept connection
                        self.acceptConnection()
                    else:
                        # Reject connection -> Connection will be automatically rejected
                        pass
                        
                elif sock in self.seedSockets:
                    # Message from seed -> Seed is not sending any message
                    pass
                
                elif sock in self.peerSockets or sock in self.peerConnections:
                    # Message from Peer
                    message = sock.recv(1024).decode()
                    if(not message):
                        self.allConnections.remove(sock)
                        self.livenessStatus[self.socketMapping[sock]] = 4
                        
                    messageParts = message.split(":")
                    
                    if(messageParts[0] == "Liveness Request"):
                        # Liveness Request
                        print(message)
                        self.writeLog(message)

                        senderTimestamp = messageParts[1]
                        senderIp = messageParts[2]
                        reply = "Liveness Reply:"+senderTimestamp+":"+senderIp+":"+str((self.ip,self.port))
                        sock.send(reply.encode())
                        
                    elif(messageParts[0] == "Liveness Reply"):
                        print(message)
                        self.writeLog(message)

                        self.livenessStatus[eval(messageParts[3])] = 0
                    
                    else:
                        # Gossip Message
                        newMessage = Message(message, sock.getpeername())
                        duplicateMessage = False
                        
                        for mes in self.messageList:
                            # Message duplicacy check
                            if(newMessage.hash == mes.hash):
                                duplicateMessage = True
                                break
                            
                        if(not duplicateMessage):
                            # Forward new gossip message
                            output = "Received Gossip Message : " + message
                            print(output)
                            self.writeLog(output)

                            self.messageList.append(newMessage)
                            receivers = self.peerConnections+self.peerSockets
                            self.sendMessage(message, receivers)

    def writeLog(self, content):
        file_path = "peer_log_" + str(self.port)+'.txt'
        with open(file_path, 'a') as file:
            file.write(content)
            file.write('\n')
            
    def __del__(self) -> None:
        self.listeningSocket.close()
        
        
def main():
    #ip = input("Enter Peer IP Address : ")
    ip = "127.0.0.1"
    port = int(input("Enter Peer Port Number : "))
    peer = PeerNode(ip, port)
    peer.setup()
    peer.establishConnections()
    peer.activate()
    
if __name__ == "__main__":
    main()