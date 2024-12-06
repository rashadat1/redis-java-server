import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.time.LocalDateTime;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

// Build Redis
public class EventLoopServer {
	private static final int port = 6379;
	
	public static Map<String, String> data = new ConcurrentHashMap<>();
	public static Map<String, LocalDateTime> expiryTimes = new ConcurrentHashMap<>();
	private static String dir = null;
	private static String dbfilename = null;
	
	private static void loadRdbFile() throws IOException {
		if (dir == null || dbfilename == null) {
			System.out.println("No RDB File specified. Skipping loading of RDB file.");
			return;
		}
		String filepath = dir + "/" + dbfilename;
		java.io.File rdbfile = new java.io.File(filepath);
		if (!rdbfile.exists()) {
			System.out.println("No RDB File exists at this path: " + filepath);
			return;
		}
		try (java.io.FileInputStream fileInputStream = new java.io.FileInputStream(rdbfile)) {
			// fileinputstream opens a connection to  the file and obtains input bytes the file, used for reading streams of raw bytes
			ByteBuffer fileBuffer = ByteBuffer.allocate((int) rdbfile.length());
			// allocate a channel to read the file
			FileChannel fileChannel = fileInputStream.getChannel();
			// read a sequence of bytes from this channel into the given buffer
			fileChannel.read(fileBuffer);
			// flips buffer limit set to the current position and then the position is set to zero - after a sequence of channel-read
			// use this method to prepare for a sequence of channel-write operatons
			fileBuffer.flip();
			// parse RDB file
			parseRDB(fileBuffer);
			
		}
		
	}
	private static void parseRDB(ByteBuffer fileBuffer) {
		// Parse header by creating an empty byte array to store the first 9 bytes of the file (which is the header)
		byte[] header = new byte[9];
		// copy data into the header array
		fileBuffer.get(header);
		String headStr = new String(header); // now header holds the first 9 bytes of the file - the header
		if (!headStr.equals("REDIS0011")) {
			throw new IllegalArgumentException("Invalid RDB header: " + headStr);
		}
		System.out.println("RDB header validated.");
		
		// iterate through sections of the file
		while (fileBuffer.hasRemaining()) {
			// check if there are any bytes between the current position and the limit in the buffer
			byte sectionType = fileBuffer.get();
			if (sectionType == (byte) 0xFA) {
				// 0x denotes hexadecimal notation and FA represents a single byte in hexadecimal
				// 0xFA indicates the start of a metadata subsection
				skipString(fileBuffer); // skip metadata key (name of the attribute)
				skipString(fileBuffer); // skip metadata value (value of the attribute)
			} else if (sectionType == (byte) 0xFE) {
				// parse database section
				parseDatabase(fileBuffer);
			} else if (sectionType == (byte) 0xFF) {
				// end of file section
				System.out.println("RDB parsing completed.");
				break;
			} else  {
				throw new IllegalArgumentException("Uknown RDB section type: " + sectionType);
			}
		}
	}
	private static void parseDatabase(ByteBuffer fileBuffer) {
		int dbIndex = readSizeEncoded(fileBuffer);
		System.out.println("Database index: " + dbIndex);
	}
	private static int readSizeEncoded(ByteBuffer fileBuffer) {
		// for efficiency we store the index using variable-length encoding
		// small numbers - 1 byte, larger numbers - 2 bytes, very large numbers - full 8 byte integer
		
		// read first byte
		byte firstByte = fileBuffer.get();
		int size = firstByte & 0x3F;
		// isolates the first two bits of the byte
		int prefix = firstByte & 0xC0;
		if (prefix == 0x80) {
			// 0x80 = 10_000_000 (hexadecimal = binary)
			// this prefix means a large value - if the prefix = 10
			return fileBuffer.getInt();
		}
		
		
	}

	public static void main(String[] args) throws ClosedChannelException {
		try {
			// create Selector for monitoring channels   
			Selector selector = Selector.open();
			// create a non-blocking server socket channel
			ServerSocketChannel serverChannel = ServerSocketChannel.open();
			serverChannel.configureBlocking(false);
			// bind the server channel to the specified port and register
			// the channel with the selector
			serverChannel.bind(new InetSocketAddress(port));
			serverChannel.register(selector,SelectionKey.OP_ACCEPT);
			ByteBuffer buffer = ByteBuffer.allocate(256);
			System.out.println("Server is running on port " + port);
			if (args.length > 0) {
				dir = args[1];
				dbfilename = args[3];
			}
			// Event loop
			while (true) {
				// Select ready channels using the selector 
				selector.select();
				// Get the set of selected keys corresponding to ready channels
				Set<SelectionKey> selectedKeys = selector.selectedKeys();
				// create an iterator object to iterate through the selectedKeys
				Iterator<SelectionKey> keyIterator = selectedKeys.iterator();
				
				while (keyIterator.hasNext()) {
					SelectionKey currKey = keyIterator.next();
					if (currKey.isAcceptable()) {
						// if the current selection key corresponds to a channel that is registered
						// with the OP_ACCEPT event then we create a new client connection
						ServerSocketChannel server = (ServerSocketChannel) currKey.channel();
						// because the channel is nonBlocking, we do not hold up the main thread waiting for a new connection
						SocketChannel clientChannel = server.accept();
						if (clientChannel != null) {
							// if there is no client awaiting connection then we immediately return null
							// if not clientChannel is a reference to the connection with the client
							clientChannel.configureBlocking(false);
							// Register for read events
							clientChannel.register(selector,  SelectionKey.OP_READ);
							System.out.println("New client connected: " + clientChannel.getRemoteAddress());
						} else {
							continue;
						}
					}
					if (currKey.isReadable()) {
						// check if event on the channel is a READ event
						SocketChannel clientChannel = (SocketChannel) currKey.channel();
						// Read data from the client
						buffer.clear();
						// clears the byte buffer to prepare it for a new read operation
						int bytesRead = clientChannel.read(buffer); // .read returns the number of bytes read - if no data is available returns 0, if client closed connection return -1
						if (bytesRead == -1) {
							System.out.println("Client disconnected: " + clientChannel.getRemoteAddress());
							currKey.cancel(); // cancel the key and remove it from the Selector
							clientChannel.close();
							continue; // continue to the next key
						}
						buffer.flip(); 
						// prepares the ByteBuffer for reading the data it has received - set buffer limit to the current position
						// and resets the position to 0 so we can start reading at the beginning of the buffer
						String message = new String(buffer.array(), 0, bytesRead);
						// buffer.array is the byte array backing the buffer 0 and bytesRead are the starting and ending points of the read
						System.out.println("Message Received " + message);
						String[] parts = message.split("\r\n");
						if (parts.length >= 2) {
							String command = parts[2].toUpperCase();
							ByteBuffer responseBuffer = null;
							switch(command) {
								case "PING":
									responseBuffer = ByteBuffer.wrap("+PONG\r\n".getBytes());
									clientChannel.write(responseBuffer);
									break;
								case "ECHO":
									System.out.println("Echo Argument: " + parts[4]);
									// parts[3] = $[length of echo argument]
									responseBuffer = ByteBuffer.wrap((parts[3] + "\r\n" + parts[4] + "\r\n").getBytes());
									clientChannel.write(responseBuffer);
									break;
								case "SET":
									// Set command without PX
									System.out.println("Set Command key: " + parts[4] + "\r\nSet Command value: " + parts[6]);
									if (parts.length == 7) {
										data.put(parts[4], parts[6]);
										responseBuffer = ByteBuffer.wrap("+OK\r\n".getBytes());
										expiryTimes.remove(parts[4]); // remove any existing expiration for this key
										
									} else if (parts.length == 11 && parts[8].equalsIgnoreCase("PX")) {
										// Set command with PX to set expiration
										data.put(parts[4],  parts[6]);
										long expiryMillis = 1_000_000 * Long.parseLong(parts[10]);
										LocalDateTime expiryTime = LocalDateTime.now().plusNanos(expiryMillis);
										expiryTimes.put(parts[4], expiryTime); // store the expiration time
										responseBuffer = ByteBuffer.wrap("+OK\r\n".getBytes());
									}
									clientChannel.write(responseBuffer);
									break;
								case "GET":
									LocalDateTime expirationTime = expiryTimes.get(parts[4]);
									if (expirationTime != null) {
										if (LocalDateTime.now().isAfter(expirationTime)) {
											// the case where the access occurs after the expiration time
											responseBuffer = ByteBuffer.wrap("$-1\r\n".getBytes());
											clientChannel.write(responseBuffer);
											break;
										}
									}
									String key = data.get(parts[4]);
									if (key == null) {
										responseBuffer = ByteBuffer.wrap("$-1\r\n".getBytes());
									} else {
										responseBuffer = ByteBuffer.wrap(("$" + key.length() + "\r\n" + key + "\r\n").getBytes());
									}
									clientChannel.write(responseBuffer);
									break;
								case "CONFIG":
									String commandStr = null;
									String paramValue = null;
									String prefix = "*2\r\n";
									String parameterName = parts[6];
									
									if (parts[4].equals("GET")) {
										// use .equals() for string comparison "==" is for reference comparison
										System.out.println("Received CONFIG GET Command");
										if (parameterName.equals("dir")) {
											commandStr = "$3\r\ndir\r\n";
											paramValue = dir != null ? dir : "";
											
										} else if (parameterName.equals("dbfilename")) {
											commandStr = "$10\r\ndbfilename\r\n";
											paramValue = dbfilename != null ? dbfilename: "";
										}
									responseBuffer = ByteBuffer.wrap((prefix + commandStr + "$" + paramValue.length() + "\r\n" + paramValue + "\r\n").getBytes());
									clientChannel.write(responseBuffer);
									}
									break;
								default:
									break;
									
							}
						}
					}
				}
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
}