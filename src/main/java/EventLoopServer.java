import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;

/* 
nio = non-blocking io
implementation of a server that uses a Event Loop instead of a new thread
for each new client that connects
*/

public class EventLoopServer {
    private static final int port = 6379;
    public static void main(String[] args) throws ClosedChannelException {
        try {
            // create a selector for monitoring channels
            Selector selector = Selector.open();
            // create a non-blocking serversocketChannel
            ServerSocketChannel serverChannel = ServerSocketChannel.open();
            serverChannel.configureBlocking(false);

            // Bind the server channel to the specified port
            serverChannel.bind(new InetSocketAddress(port));
            // register channel with the selector to listen for OP_ACCEPT events
            serverChannel.register(selector, SelectionKey.OP_ACCEPT);
            ByteBuffer buffer = ByteBuffer.allocate(256);
            System.out.println("Server is running on port " + port);

            // Event loop
            while (true) { 
                // Select ready channels using the selector
                selector.select();
                // Get the set of selected keys
                Set<SelectionKey> selectedKeys = selector.selectedKeys();
                Iterator<SelectionKey> keyIterator = selectedKeys.iterator();

                while (keyIterator.hasNext()) {
                    SelectionKey key = keyIterator.next();
                    if (key.isAcceptable()) {

                        // Accept a new connection from a client
                        ServerSocketChannel server = (ServerSocketChannel) key.channel();
                        SocketChannel clientChannel = server.accept();
                        clientChannel.configureBlocking(false);
                        // Register for read events
                        clientChannel.register(selector, SelectionKey.OP_READ);
                        System.out.println("New client connected: " + clientChannel.getRemoteAddress());
                    }
                    if (key.isReadable()) {
                        SocketChannel clientChannel = (SocketChannel) key.channel();
                        // Read data from the client
                        buffer.clear();
                        int bytesRead = clientChannel.read(buffer);
                        if (bytesRead == -1) {
                            // Client disconnected
                            key.cancel();
                            clientChannel.close();
                            System.out.println("Client disconnected: " + clientChannel.getRemoteAddress());
                            continue;
                        }
                    

                        buffer.flip();
                        String message = new String(buffer.array(), 0, bytesRead);
                        // Process received message
                        System.out.println("Received: " + message);

                        // Respond to PING messages
                        if (message.equalsIgnoreCase("PING")) {
                            ByteBuffer responseBuffer = ByteBuffer.wrap("+PONG\r\n".getBytes());
                            clientChannel.write(responseBuffer);
                        }
                     
                    }
                }
            } 
            } catch (IOException e) {
                e.printStackTrace();
            }   
    }
}
