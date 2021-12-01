# Twitter_Simulator
Implemented a Twitter clone engine and an Actor-model based client tester/simulator. 

# Team Members
1. Akshay Ganapathy (UFID - 3684-6922)
2. Kamal Sai Raj Kuncha (UFID - 4854-8114)

# Requirements
Input:
The input to the program is the server IP address, the port of the server machine where the engine is currently running, and the number of clients the machine is to handle.

Output:
The output is the performance of various aspects of the simulator such as get tweets and retweets, get mentions, get hashtags.

# Zip File Contents
The zip file consists of the readme.md file, Project_Report.pdf file, proj4_server.fsx file and the proj4_client.fsx file, which contains the code to be run.

# How To Run
Run and start the server first using the following command:
Server : dotnet fsi –langversion:preview proj4_server.fsx 

After the server program displays ‘Server started’, then run the following command.

Client : dotnet fsi –langversion:preview proj4_client.fsx <server_ip> <server_port> <number_of_clients>

where 	‘server_ip’ is the IP address of the server,
‘server_port’ is the Port number in which the server is running, and
‘number_of_clients is the number of clients the machine is to handle.

# Largest network managed

On running the code, the largest number of clients we were able to work with was 2000. 
To work with even larger clients would have led to us obtaining results but with time consuming delays. 
In addition, we also managed to run the program by connecting the server with multiple clients (3) and it worked as expected.