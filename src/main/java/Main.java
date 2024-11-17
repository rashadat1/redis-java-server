import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.io.InputStreamReader;

public class Main {
  public static void main(String[] args){
    System.out.println("Logs from your program will appear here!");
    
        ServerSocket serverSocket = null;
        Socket clientSocket = null;
        int port = 6379;
        try {
        	serverSocket = new ServerSocket(port);
        	serverSocket.setReuseAddress(true);
        	
        	clientSocket = serverSocket.accept();
        	
        	// for writing data to the output stream of the clientSocket (sending)
        	PrintWriter out = new PrintWriter(clientSocket.getOutputStream(),true);
        	// for reading from the input stream of the clientSocket (receiving)
        	BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        	// write REDIS protocol encoded string PONG to the output Stream of the clientSocket
        	out.print("+PONG\\r\\n");
        			
        } catch (IOException e) {
          System.out.println("IOException: " + e.getMessage());
        } finally {
          try {
            if (clientSocket != null) {
              clientSocket.close();
            }
          } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
          }
        }
  }
}

/*
In this code we create a TCP server that listens on port 6379
A ServerSocket called serverSocket is created to listen for incoming
client connections on port 6379

.setReuseAddress(true) is a socket option that allows the server to reuse a port
immediately after the program terminates. Without this - because of wait time state
the server might respond with a "Address already in use" warning if restarted too quickly

The server remains in a blocked state at serverSocket.accept()
until a client conects. Once a client connects a Socket object is created
(clientSocket). The server could then use this socket to communicate with the client
(send or receive data)
*/