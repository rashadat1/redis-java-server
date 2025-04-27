import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class Main {
  public static void main(String[] args){
    System.out.println("Logs from your program will appear here!");
        int port = 6379;
        try (ServerSocket serverSocket = new ServerSocket(port)) {
          serverSocket.setReuseAddress(true);
          System.out.println("Server listening on port " + port);

          while (true) {
            // Accept a new client
            // blocking call waiting for new client connection but only affects the main thread
            Socket clientSocket = serverSocket.accept();
            ClientHandler clientHandler = new ClientHandler(clientSocket);
            // main thread only waits for and accepts new client connections
            // each connected client is handled independently by its own thread
            Thread clientThread = new Thread(clientHandler);
            clientThread.start();
          }
        } catch (IOException e) {
          System.out.println("IOException: " + e.getMessage());
        }
  }
}
// ClientHandler class to handle a single client connection
// The Runnable interface is implemented by any class whose methods are needed to
// be executed by a thread
class ClientHandler implements Runnable {
  private final Socket clientSocket;

  public ClientHandler(Socket clientSocket) {
    this.clientSocket = clientSocket;
  }
  @Override
  public void run() {
    try (
      // for writing data to the output stream of the clientSocket (sending)
      PrintWriter out = new PrintWriter(clientSocket.getOutputStream(),true);
      // for reading from the input stream of the clientSocket (receiving)
      BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));     
    ) {
      String input;
      while ((input = in.readLine()) != null) {
        System.out.println("Received: " + input);
        if (input.contains("PING")) {
          // write REDIS protocol encoded string PONG to the output Stream of the clientSocket
          out.print("+PONG\r\n");
          out.flush();
        } 
      }
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
until a client connects. Once a client connects a Socket object is created
(clientSocket). The server could then use this socket to communicate with the client
(send or receive data)
*/