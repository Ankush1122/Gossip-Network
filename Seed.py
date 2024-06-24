import socket
import select
import pickle

class SeedNode:
    def __init__(self, ip, port) -> None:
        self.ip = ip
        self.port = port
        self.socket = None              # Socket object for listening to peer nodes connection request
        self.peers = []                 # Information of peer nodes address
        self.connections = []           # Connection objects of peer nodes for communication

        
    def setup(self) -> None:
        self.socket =  socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind((self.ip, self.port))
        
    def acceptConnection(self) -> None:
        clientSocket, clientAddress = self.socket.accept()
        
        # Send Peer list to new peer
        clientSocket.send(pickle.dumps(self.peers))
        data = clientSocket.recv(1024).decode()
        
        print(data)
        clientAddress = eval(data)
        
        self.peers.append(clientAddress)
        self.connections.append(clientSocket)
        
        output = "Connection accepted from  " + str(clientAddress)
        print(output)
        self.writeLog(output)
        
        
    def removeDeadNode(self, deadMessage) -> None:
        messageParts = deadMessage.split(":")
        
        if(messageParts[0] == "Dead Node"):
            deadNode = eval(messageParts[1])

            if(deadNode in self.peers):
                self.peers.remove(deadNode)
                
    
    def activate(self) -> None:
        self.connections = [self.socket]
        self.socket.listen()
        
        while True:
            readable, _, _ = select.select(self.connections, [], [])

            for sock in readable:
                if sock == self.socket:
                    # Connection Request
                    self.acceptConnection()
                else:
                    # Dead Node Message
                    message = sock.recv(1024).decode()
                    if(not message):
                        self.connections.remove(sock)
                        sock.close()
                        continue
                    self.removeDeadNode(message)
                    
                    print(message)
                    self.writeLog(message)

    
    def writeLog(self, content):
        # Create and write output to log file
        file_path = "seed_log_" + str(self.port)+'.txt'
        with open(file_path, 'a') as file:
            file.write(content)
            file.write('\n')
    
    def __del__(self) -> None:
        self.socket.close()
        
        
def main():
    #ip = input("Enter Seed IP Address : ")
    ip = "127.0.0.1"
    port = int(input("Enter Seed Port Number : "))
    seed = SeedNode(ip, port)
    seed.setup()
    seed.activate()
    
if __name__ == "__main__":
    main()