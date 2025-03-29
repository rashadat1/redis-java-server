import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
// Build Redis
public class EventLoopServer {
	
	private static int port = 6379;
	public static Map<String, Object> data = new ConcurrentHashMap<>();
	public static Map<String, LocalDateTime> expiryTimes = new ConcurrentHashMap<>();
	private static String dir = null;
	private static String dbfilename = null;
	private static boolean isreplica = false;
	public static String master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
	public static int master_repl_offset = 0;
	private static String master_host = null;
	private static int master_port = 0;
    final static String empty_rdb_contents = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
    final static List<SocketChannel> replicaSocketList = new CopyOnWriteArrayList<>();
	private static int replConfOffset = 0;
    private static int acknowledgedReplicas = 0;
    private static int writeOffset = 0;
    private static WaitRequest waitRequest = null;
    private static int parsedREPLACK = 0;
    final static HashMap<String, Stream> streamMap = new HashMap<>();
	private static void loadRDBFile() throws IOException {
		if (dir == null | dbfilename == null) {
			System.out.println("No RDB file specified.");
			return;
		}
		String pathname = dir + '/' + dbfilename;
		java.io.File rdbFile = new java.io.File(pathname);
		if (!rdbFile.exists()) {
			System.out.println("RDB File does not exist at the specified filepath: " + pathname);
			return;
		}
		// A FileInputStream obtains input bytes from a file in a file system
		try (java.io.FileInputStream fileInputStream = new java.io.FileInputStream(rdbFile)) {
			// return the unique file channel for the given input stream
			FileChannel fileChannel = fileInputStream.getChannel();
			ByteBuffer fileBuffer = ByteBuffer.allocate((int) rdbFile.length());
			// after creating the file channel we read the bytes from the fileInputStream to the fileBuffer using the fileChannel
			fileChannel.read(fileBuffer);
			// flip the fileBuffer so that our cursor is at the beginning of the buffer
			fileBuffer.flip();
			parseRDB(fileBuffer);
			
		}
	}
	
	private static void parseRDB(ByteBuffer fileBuffer) {
		// in hexadecimal the header section of RDB file looks like: 52 45 44 49 53 30 30 31 31
		// each hexadecimal digit is 4 bits so this is 9 bytes
		byte[] header = new byte[9];
		// reads the bytes at the current position into the header byte array and increments position
		fileBuffer.get(header);
		String headerStr = new String(header);
		if (!headerStr.equals("REDIS0011")) {
			System.out.println("Incorrect RDB File Header.");
			return;
		}
		System.out.println("RDB Header Validated: " + headerStr);
		System.out.println("After header validation, buffer position: " + fileBuffer.position());
		while (fileBuffer.hasRemaining()) {
			byte sectionType = fileBuffer.get();
			if (sectionType == (byte) 0x00) {
				// 00 corresponds to a string encoded key-value pair
				String key = readString(fileBuffer);
				String value = readString(fileBuffer);
				if (!key.isEmpty()) {
					data.put(key, value);
				}
				
			} else if (sectionType == (byte) 0xFD) {
				// key-value with expiry time in seconds
				long expiryTime = readTimeSeconds(fileBuffer);
				readString(fileBuffer);
				
				String key = readString(fileBuffer);
				String value = readString(fileBuffer);
				LocalDateTime expirationTime = LocalDateTime.ofEpochSecond(expiryTime, 0, java.time.ZoneOffset.UTC);
				
				System.out.println("Loaded Key: " + key + " Value: " + value + " Expiry: " + expirationTime);

				if (!key.isEmpty()) {
					data.put(key, value);
					expiryTimes.put(key, expirationTime);
				}
			} else if (sectionType == (byte) 0xFC) {
				// key-value with expiry time in milliseconds
				long expiryTimeMillis = readTimeMS(fileBuffer);
				readString(fileBuffer);

				String key = readString(fileBuffer);
				String value = readString(fileBuffer);
				// convert ms to seconds then pass remainder in nanoseconds as second argument   
				LocalDateTime expirationTime = LocalDateTime.ofEpochSecond(expiryTimeMillis / 1000, (int) (expiryTimeMillis % 1000) * 1_000_000, java.time.ZoneOffset.UTC);
				
				System.out.println("Loaded Key: " + key + " Value: " + value + " Expiry: " + expirationTime);

				if (!key.isEmpty()) {
					data.put(key, value);
	                expiryTimes.put(key, expirationTime);
				}
			} else if (sectionType == (byte) 0xFE) {
				// FE 00 indicates a database selector followed by metadata
				int dbNumber = readSize(fileBuffer);
				System.out.println("Database Selector: DB " + dbNumber);
				continue;
			
			} else if (sectionType == (byte) 0xFB) {
				// Indicates fields for the size of the hash tables after FE 00
				readSize(fileBuffer);
				readSize(fileBuffer);
			
			} else if (sectionType == (byte) 0xFF) {
				break;
			}
			
		}
	}
	
	private static String readString(ByteBuffer fileBuffer) {
		int size = readSize(fileBuffer);
		System.out.println("Parsed size: " + size);
		byte[] stringBytes = new byte[size];
		fileBuffer.get(stringBytes);
		String bytesToString = new String(stringBytes);
		return bytesToString;
	}
	
	private static long readTimeSeconds(ByteBuffer fileBuffer) {
		byte[] rawBytes = new byte[4];
		fileBuffer.get(rawBytes); // read 4 bytes from the buffer
		ByteBuffer buffer = ByteBuffer.wrap(rawBytes).order(ByteOrder.LITTLE_ENDIAN); // convert to Little-Endian .wrap() converts to ByteBuffer to allow use of this classes methods
		
		int unsigned_int = buffer.getInt() & 0xFFFFFFFF; // convert to unsigned
		long unixTimeStamp = unsigned_int & 0xFFFFFFFFL; // handle as long
		
		System.out.println("Parsed Expiry Time in S (little-endian): " + unixTimeStamp);
		return unixTimeStamp;
	}
	
	private static long readTimeMS(ByteBuffer fileBuffer) {
		// Java long is already 8 bytes so no need to mask
		byte[] rawBytes = new byte[8];
		fileBuffer.get(rawBytes); // read 8 bytes from the buffer
		ByteBuffer buffer = ByteBuffer.wrap(rawBytes).order(ByteOrder.LITTLE_ENDIAN);  // convert to Little-Endian
		long unsigned_long = buffer.getLong();
		
		System.out.println("Parsed Expiry Time in MS (little-endian): " + unsigned_long);
		return unsigned_long;
		
	}
	
	private static int readSize(ByteBuffer buffer) {
	    byte firstByte = buffer.get();
	    int firstTwoBits = (firstByte & 0xC0) >> 6;
	    
	    if (firstTwoBits == 0) {
	        // 0b00 - 6-bit size
	        return firstByte & 0x3F;
	    } else if (firstTwoBits == 1) {
	        // 0b01 - 14-bit size (6 bits from first byte + 8 bits from second byte)
	        int secondByte = buffer.get() & 0xFF;
	        return ((firstByte & 0x3F) << 8) | secondByte;
	    } else if (firstTwoBits == 2) {
	        // 0b10 - 32-bit size (next 4 bytes as a full integer)
	        return buffer.getInt();
	    } else if (firstTwoBits == 3) {
	        // 0b11 - Special encoding
	        int encodingType = firstByte & 0x3F; // The remaining 6 bits tell us the type
	        System.out.println("Special encoding type: " + Integer.toHexString(encodingType));
	        switch (encodingType) {
	            case 0x00: // 0xC0 - 8-bit integer encoding
	                return buffer.get();
	            case 0x01: // 0xC1 - 16-bit integer encoding (little-endian)
	                return buffer.getShort() & 0xFFFF; // convert short to unsigned int
	            case 0x02: // 0xC2 - 32-bit integer encoding (little-endian)
	                return buffer.getInt();
	            default:
	            	System.out.println("Unknown encoding type: " + encodingType);
	            	buffer.position(buffer.position() + 1);
	            	return 0;
	        }
	    } else {
	        System.out.println("Unsupported size encoding");
	        return 0;
	    }
	}
    private static byte[] decodeHex(String rdbContents) {
        int length = rdbContents.length();
        byte[] data = new byte[length / 2]; // each hex digit (0-9, A-F) is 4 bits so we map pairs to a singular bytes
        for (int i = 0; i < length; i+=2) {
            // process two characters at a time to convert them into a single byte of data
            // hex chars 0 and 1 -> first byte, hex chars 2 and 3 -> second byte, etc
            // Character.digit returns numerical value of the character in the given base so 'A' would return 10, 'F' -> 15
            data[i/2] = (byte) ((Character.digit(rdbContents.charAt(i),16) << 4) + Character.digit(rdbContents.charAt(i+1),16));
            // shift upper 4 bits up by 4 bits
        }
        return data;
    }
    public static void parseArgs(String[] args) {
    	// parse command line arguments - these include the path for the rdb file, the port we are setting up the server on
		// and whether the server is a leader or follower
		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("--port")) {
				port = Integer.parseInt(args[i+1]);
			} else if (args[i].equals("--dir")) {
				dir = args[i+1];
			} else if (args[i].equals("--dbfilename")) {
				dbfilename = args[i+1];
			} else if (args[i].equals("--replicaof")) {
				isreplica = true;
				try {
					String[] serverLocation = args[i+1].split(" ");
					master_host = serverLocation[0];
					master_port = Integer.parseInt(serverLocation[1]);
				} catch (ArrayIndexOutOfBoundsException | NumberFormatException e) {
					System.out.println("Invalid --replicaof format. Expected: \"<host> <port>\"");
					isreplica = false; // disable if parsing fails so we do not advance
				}
			}
		}
    }
	public static void main(String[] args) throws ClosedChannelException {
		try {
			parseArgs(args);
			
			// create Selector for channel monitoring  
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
		
			
			if (isreplica && master_host != null && master_port != 0) {
				try {
					System.out.println("Attempting to establish a connection with master at " + master_host + ":" + master_port);
					SocketChannel masterChannel = SocketChannel.open();
					masterChannel.connect(new InetSocketAddress(master_host, master_port));
					
					String PingCommand = "*1\r\n$4\r\nPING\r\n";
					masterChannel.write(ByteBuffer.wrap(PingCommand.getBytes()));
					
					// read PONG received from Master. If PONG received sends REPLCONF twice to the master
					ByteBuffer masterBuffer = ByteBuffer.allocate(256);
					// allocate 256 byte sized buffer
					int BytesReadFromMaster = masterChannel.read(masterBuffer);
					// read from masterChannel into the masterBuffer 
					masterBuffer.flip();
					String PingResponse = new String(masterBuffer.array(), 0, BytesReadFromMaster);
					masterBuffer.clear();
					System.out.println("Master Response to PING: " + PingResponse);
					String[] PingParts = PingResponse.split("\r\n");
					if (PingParts[0].equalsIgnoreCase("+PONG")) {
						// if true then master responded correctly with PONG
						System.out.println("Sending First ReplConf");
						String firstReplConf = "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n" + port + "\r\n";
						masterChannel.write(ByteBuffer.wrap(firstReplConf.getBytes()));
						
						int BytesReadReplConf = masterChannel.read(masterBuffer);
						masterBuffer.flip();
						String FirstReplConfResponse = new String(masterBuffer.array(), 0, BytesReadReplConf);
						masterBuffer.clear();
						if (!FirstReplConfResponse.contains("OK")) {
							System.out.println("Failed to receive OK response from Master for REPLCONF 1");
						} else {
							System.out.println("Received: " + FirstReplConfResponse + "in response to first REPLCONF command");
							System.out.println("Sending second REPLCONF command");
							String secondReplConf = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
							masterChannel.write(ByteBuffer.wrap(secondReplConf.getBytes()));
							
							int BytesReadSecondReplConf = masterChannel.read(masterBuffer);
							masterBuffer.flip();
							String SecondReplConfResponse = new String(masterBuffer.array(), 0, BytesReadSecondReplConf);
							if (!SecondReplConfResponse.contains("OK")) {
	                            System.out.println("Failed to receive OK response from Master for REPLCONF 2");
	                        } else {
	                            System.out.println("Master Server replied with OK to both REPLCONF commands");
	                            masterBuffer.clear();
	                                
	                            System.out.println("Sending PSYNC to the master");
	                            String PsyncCommand = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
	                            masterChannel.write(ByteBuffer.wrap(PsyncCommand.getBytes()));
	                            masterBuffer.clear();
	                            
	                            System.out.println("Handshake complete - Registering Master Channel with Selector");
	        					masterChannel.configureBlocking(false);
	                            masterChannel.register(selector, SelectionKey.OP_READ, "master");
	                            
	                        }
						}
											
	                }	
				} catch (IOException e) {
					System.err.println("Error during master handshake: " + e.getMessage());
				}
			}
            loadRDBFile();
			for (String arg : args ) {
				System.out.println("Arguments: " + arg);
			}
			// Event loop
			while (true) {
                long currentTime = System.currentTimeMillis();
                long blockTime = 0;
                if (waitRequest != null) {
                    // check if we are processing a wait command - if so we should check to see if the timeout has been reached
                    blockTime = waitRequest.timeOut - System.currentTimeMillis();
                    if (currentTime >= waitRequest.timeOut) {
                        System.out.println("Wait request time out reached");
                        ByteBuffer waitResponseBuffer = ByteBuffer.wrap((":" + acknowledgedReplicas + "\r\n").getBytes());
                        waitRequest.channel.write(waitResponseBuffer);

                        waitRequest = null;
                        master_repl_offset += "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n1\r\n*\r\n".getBytes().length;
                        writeOffset = 0;
                        acknowledgedReplicas = 0;
                        break;
                    }

                }
				// Select ready channels using the selector
				selector.select(Math.max(blockTime, 0)); // if we have passed the timeOut deadline then continue as normal
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
							clientChannel.register(selector,  SelectionKey.OP_READ, "client");
							System.out.println("New client connected: " + clientChannel.getRemoteAddress());
						} else {
							continue;
						}
					} else if (currKey.isReadable()) {
						// check if event on the channel is a READ event
						SocketChannel channel = (SocketChannel) currKey.channel();
						String sourceType = (String) currKey.attachment(); // retrieve metadata - master or client
						System.out.println("Reading from " + sourceType + " channel" + channel.getRemoteAddress());

                        buffer.clear();
						// clears the byte buffer to prepare it for a new read operation
						int bytesRead = channel.read(buffer); // read returns the number of bytes read - if no data is available returns 0, if client closed connection return -1
						if (bytesRead == -1) {
							System.out.println("Channel disconnected: " + channel.getRemoteAddress());
							currKey.cancel(); // cancel the key and remove it from the Selector
							channel.close();
							continue; // continue to the next key
						}
						buffer.flip();
                        System.out.println(sourceType + " wrote " + bytesRead + " bytes");
						// prepares the ByteBuffer for reading the data it has received - set buffer limit to the current position
						// and resets the position to 0 so we can start reading at the beginning of the buffer
						String message = new String(buffer.array(), 0, bytesRead);
						// buffer.array is the byte array backing the :wantbuffer 0 and bytesRead are the starting and ending points of the read
						String[] subcommands = message.split("(?=\\*[0-9])");
						for (String subcommand : subcommands) {
							if (subcommand.isEmpty()) {
								continue;
							}
							// subcommand = "*" + subcommand;
							System.out.println("Received subcommand: " + subcommand);
							String[] parts = subcommand.split("\r\n");
							
							if (parts.length >= 2) {
								String command = parts[2].toUpperCase();
								ByteBuffer responseBuffer = null;
                                if (!subcommand.contains("FULLRESYNC") && !subcommand.contains("GETACK")) {

                                    replConfOffset = replConfOffset + subcommand.getBytes().length;

                                }
                                
								switch(command) {
									case "PING":
                                        String pingResponse = "+PONG\r\n";
										responseBuffer = ByteBuffer.wrap(pingResponse.getBytes());
                                        if (!isreplica) {
										    channel.write(responseBuffer);
                                            master_repl_offset += pingResponse.getBytes().length;
                                        }
										break;
									case "ECHO":
										System.out.println("Echo Argument: " + parts[4]);
                                        String echoResponse = parts[3] + "\r\n" + parts[4] + "\r\n"; 
										responseBuffer = ByteBuffer.wrap(echoResponse.getBytes());
                                        if (!isreplica) {
										    channel.write(responseBuffer);
                                            master_repl_offset += echoResponse.getBytes().length;
                                        }
										break;
									case "SET":
										// Set command without PX
										System.out.println("Set Command key: " + parts[4] + "\r\nSet Command value: " + parts[6]);
										if (parts.length == 7) {
											if (!parts[4].isEmpty()) {
												data.put(parts[4], parts[6]);
											}
	                                         
											responseBuffer = ByteBuffer.wrap("+OK\r\n".getBytes());
											expiryTimes.remove(parts[4]); // remove any existing expiration for this key
											
										} else if (parts.length == 11 && parts[8].equalsIgnoreCase("PX")) {
											// Set command with PX to set expiration
											if (!parts[4].isEmpty()) {
												data.put(parts[4],  parts[6]);
											}
											long expiryMillis = 1_000_000 * Long.parseLong(parts[10]);
											LocalDateTime expiryTime = LocalDateTime.now().plusNanos(expiryMillis);
											expiryTimes.put(parts[4], expiryTime); // store the expiration time
											responseBuffer = ByteBuffer.wrap("+OK\r\n".getBytes());
										}
	                                    if (!isreplica) {
	                                        // if this is the master we send the "+OK" response back to the client making the request
	                                        // in addition we propagate the request to the replicas in the list
                                            writeOffset += 1;
										    channel.write(responseBuffer);
                                            master_repl_offset += message.getBytes().length;
	                                        for (SocketChannel channelreplica : replicaSocketList) {
                                                System.out.println("Propagating Write to replica: " + channelreplica.getRemoteAddress());
	                                            channelreplica.write(ByteBuffer.wrap(message.getBytes()));
	                                        }
	                                    }
										break;
									case "GET":
										LocalDateTime expirationLocalDateTime = expiryTimes.get(parts[4]);
										if (expirationLocalDateTime != null) {
											Instant expirationTime = expirationLocalDateTime.toInstant(java.time.ZoneOffset.UTC);
											System.out.println("Expiration Time for key '" + parts[4] + "': " + expirationTime);
											System.out.println("Current Time: " + Instant.now());
											if (Instant.now().isAfter(expirationTime)) {
												// the case where the access occurs after the expiration
												responseBuffer = ByteBuffer.wrap("$-1\r\n".getBytes());
									            System.out.println("Key '" + parts[4] + "' has expired. Response Sent: $-1");
												channel.write(responseBuffer);
												break;
											}
										}
										String key = (String) data.get(parts[4]);
										if (key == null) {
											responseBuffer = ByteBuffer.wrap("$-1\r\n".getBytes());
										} else {
											responseBuffer = ByteBuffer.wrap(("$" + key.length() + "\r\n" + key + "\r\n").getBytes());
										}
										System.out.println("Key found to send to client: " + key);
										
										channel.write(responseBuffer);
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
										channel.write(responseBuffer);
										}
										break;
									case "KEYS":
                                        if (parts.length <= 4) {
                                            System.out.println("KEYS command received");
                                        } else if (parts[4].equals("*")) {
											System.out.println("KEYS * command received");
											// StringBuilder allows us to work with a mutable string so we don't have to create a new string with
											// each of these operations
                                        }
                                        StringBuilder KeysResponse = new StringBuilder("*").append(data.size()).append("\r\n");
                                        data.keySet().forEach(k -> {
                                            KeysResponse.append("$").append(k.length()).append("\r\n").append(k).append("\r\n");
                                        });
                                        responseBuffer = ByteBuffer.wrap(KeysResponse.toString().getBytes());
                                        channel.write(responseBuffer);
                                        break;
									case "INFO":
										if (parts[4].equals("replication")) {
											System.out.println("INFO Replication command received");
											StringBuilder InfoResponse = new StringBuilder("$");
											
											String role = isreplica ? "slave" : "master";
											String master_id = "";
											
	
											String info = "# Replication\r\nrole:" + role;
											if (role.equals("master")) {
												master_id += master_replid;
												info += "\r\nmaster_repl_offset:0\r\nmaster_replid:" + master_id;
											}
											InfoResponse.append(info.length()).append("\r\n").append(info).append("\r\n");
											responseBuffer = ByteBuffer.wrap(InfoResponse.toString().getBytes());
											channel.write(responseBuffer);
										}
										break;

									case "REPLCONF":
                                        if (parts.length >= 4 && parts[4].equalsIgnoreCase("GETACK")) {
                                            System.out.println("REPLCONF GETACK command received from master");
                                            
                                            StringBuilder response = new StringBuilder("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n");
                                            response.append("$" + Integer.toString(replConfOffset).length());
                                            response.append("\r\n");
                                            response.append(replConfOffset);
                                            response.append("\r\n");
                                            responseBuffer = ByteBuffer.wrap(response.toString().getBytes());
                                            channel.write(responseBuffer);
                                            System.out.println("Adding REPLCONF GETACK Bytes: " + subcommand.getBytes().length);
                                            replConfOffset += subcommand.getBytes().length;
                                        
                                        } else if (parts.length >= 4 && parts[4].equalsIgnoreCase("ACK")) {
                                            System.out.println("ACK received from replica");
                                            parsedREPLACK++;
                                            System.out.println(parsedREPLACK + "th ACK received out of " + replicaSocketList.size() + " total replicas connected");
                                            int replConfOffset = Integer.parseInt(parts[6]);
                                            
                                            if (waitRequest != null || waitRequest == null) {
                                                System.out.println("Wait Request object is: " + waitRequest);
                                                if (replConfOffset > 0) {
                                                    System.out.println("Count of replicas that are synced with master increased by 1");
                                                    acknowledgedReplicas++; // count replica if it is synced to master
                                                    System.out.println("Acknowledged Replicas: " + acknowledgedReplicas);
                                                    System.out.println("Current Time: " + System.currentTimeMillis());
                                                    System.out.println("Expiry: " + waitRequest.timeOut);
                                                }
                                                if (acknowledgedReplicas >= waitRequest.numReplicas || System.currentTimeMillis() >= waitRequest.timeOut || parsedREPLACK == replicaSocketList.size()) {
                                                    System.out.println("Wait command completed returning: " + ":" + acknowledgedReplicas + "\r\n");
                                                    responseBuffer = ByteBuffer.wrap((":" + acknowledgedReplicas + "\r\n").getBytes());
                                                    waitRequest.channel.write(responseBuffer);
                                                    waitRequest = null;
                                                    master_repl_offset += "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n1\r\n*\r\n".getBytes().length;
                                                    writeOffset = 0;
                                                    acknowledgedReplicas = 0;
                                                }
                                            }
                                            break;
                                            
                                        } else if (!isreplica){
                                            System.out.println("REPLCONF command received");
                                            responseBuffer = ByteBuffer.wrap("+OK\r\n".getBytes());
                                            // replica only respons to REPLCONF GETACKs from the master. The master will receive plain REPLCONF from replica during handshake
                                            channel.write(responseBuffer);
                                            break;
                                        } else {
                                            System.out.println("SOMETHING UNEXPECTED IS HAPPENING WITH REPLCONF");
                                        }
                                        break;
	                                case "PSYNC":
	                                    System.out.println("PSYNC command received");
                                        System.out.println("Resetting master_repl_offset to 0");
                                        master_repl_offset = 0;
	                                    String PsyncResponse = "+FULLRESYNC " + master_replid + " " + master_repl_offset + "\r\n";
	                                    responseBuffer = ByteBuffer.wrap(PsyncResponse.toString().getBytes());
	                                    channel.write(responseBuffer);
	                           
	                                    // Convert raw hex string for empty RDB file contents into raw binary data (byte array)
	                                    // get the length of the byte array then wrap the content as bytes and send the length  
	                                    // followed by the contents
	                                    byte[] decodedRdb = decodeHex(empty_rdb_contents);
	                                    String rdbHeader = "$" + decodedRdb.length + "\r\n";
	                                    // Send RDB file length header
	                                    channel.write(ByteBuffer.wrap(rdbHeader.getBytes()));
	                                    // Send RDB file contents
	                                    channel.write(ByteBuffer.wrap(decodedRdb));
	                                    System.out.println("Added Replica to list: " + channel.getRemoteAddress());
                                        // register the replica channel with the Selector
                                        channel.configureBlocking(false);
                                        channel.register(selector, SelectionKey.OP_READ, "replica");
                                        System.out.println("Replica registered with selector for OP_READ: " + channel.getRemoteAddress());
                                    	replicaSocketList.add(channel);
	                                    break;
                                    case "WAIT":
                                        System.out.println("WAIT command received");
                                        System.out.println("There are " + replicaSocketList.size() + " total replicas");
                                        if (replicaSocketList.size() == 0) {
                                            // if no replicas return immediately
                                            channel.write(ByteBuffer.wrap(":0\r\n".getBytes()));
                                            break;
                                        }
                                        int numReplicas = Integer.parseInt(parts[4]);
                                        long timeOut = Long.parseLong(parts[6]);
                                        long expiry = System.currentTimeMillis() + timeOut;
                                        // store WAIT command details
                                        System.out.println("Creating WaitRequest instance - " + "numReplicas: " + numReplicas + " expiry: " + expiry);
                                        waitRequest = new WaitRequest(numReplicas, expiry, channel);
                                        if (writeOffset == 0) {
                                            channel.write(ByteBuffer.wrap((":" + replicaSocketList.size() + "\r\n").getBytes()));
                                            break;
                                        }
                                        String replMessage = "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n";

                                        for (SocketChannel replicaSocket : replicaSocketList) {
                                            // Send REPLCONF GETACK to all replicas
                                            replicaSocket.write(ByteBuffer.wrap(replMessage.getBytes()));
                                            System.out.println("Sent REPLCONF GETACK to replica: " + replicaSocket.getRemoteAddress());
                                        }
                                        parsedREPLACK = 0;
                                        // break so we do not block here - we want the main loop to continue execution
                                        break;
                                    case "TYPE":
                                        System.out.println("TYPE command received");
                                        String keyToFind = parts[4];

                                        Object val = data.getOrDefault(keyToFind, null);
                                        Object valStream = streamMap.getOrDefault(keyToFind, null);
                                        StringBuilder typeResponse = new StringBuilder("+");
                                        if (val == null && valStream == null) {
                                            typeResponse.append("none");
                                        } else if (val instanceof String) {
                                            typeResponse.append("string");
                                        } else if (valStream instanceof Stream) {
                                            typeResponse.append("stream");
                                        }
                                        typeResponse.append("\r\n");
                                        String typeResponseString = typeResponse.toString();
                                        System.out.println("Sending the following response: " + typeResponseString);
                                        channel.write(ByteBuffer.wrap(typeResponseString.getBytes()));
                                        break;
                                    case "XADD":
                                        System.out.println("XADD command received");
                                        String streamName = parts[4];
                                        String streamId = parts[6];
                                        System.out.println("The streamID is: " + streamId);
                                        
                                        boolean fullyAutoGeneratedId = (streamId.equals("*"));
                                        String[] entryParts = streamId.split("-");
                           
                                        HashMap <String, String> streamData = new HashMap<>();
                                        for (int i = 8; i + 2 < parts.length; i += 4) {
                                            String streamKey = parts[i];
                                            String streamVal = parts[i + 2];
                                            streamData.put(streamKey, streamVal);
                                        }
                                        boolean result = true;
                                        boolean partiallyAutoGeneratedId = (!entryParts[0].equals("*") && entryParts[1].equals("*"));
                                        if (!streamMap.containsKey(streamName)) {
                                            System.out.println("Creating new stream with stream name " + streamName);
                                            // if there is no stream with this name we create a new one
                                            Stream newStream = new Stream();
                                            if (partiallyAutoGeneratedId) {
                                                System.out.println("Detected partiallyAutoGeneratedId");
                                                if (entryParts[0].equals("0")) {
                                                    streamId = "0-1";
                                                } else {
                                                    streamId = entryParts[0] + "-0";
                                                }
                                            } else if (fullyAutoGeneratedId) {
                                                System.out.println("Detected fullyAutoGeneratedId");
                                                streamId = String.valueOf(System.currentTimeMillis()) + "-0";
                                            }
                                            StreamNode newStreamNode = new StreamNode(streamId, streamData);
                                            result = newStream.insertNewNode(newStreamNode);
                                            if (result) {
                                                System.out.println("Adding StreamName " + streamName + " to the streamMap");
                                                streamMap.put(streamName, newStream);
                                            }
                                        } else {
                                            // if there is a stream with this name we try to insert the new node 
                                            Stream retrievedStream = streamMap.get(streamName);
                                            String lastId = retrievedStream.lastID;
                                            if (partiallyAutoGeneratedId) {
                                                System.out.println("Detected partiallyAutoGeneratedId");
                                                // if the id is of the form Long-*
                                                if (lastId.split("-")[0].equals(entryParts[0])) {
                                                    // if the last entered id (the largest) is a match - increment the sequence number of the lastId
                                                    streamId = entryParts[0] + "-" + String.valueOf(Long.parseLong(lastId.split("-")[1]) + 1);
                                                } else {
                                                    // if the last entered id is not a match just create a new id with sequence number 0
                                                    streamId = entryParts[0] + "-0";
                                                } 
                                            } else if (fullyAutoGeneratedId) {
                                                System.out.println("Detected fullyAutoGeneratedId");
                                                long timeForId = System.currentTimeMillis();
                                                streamId = (timeForId > Long.parseLong(lastId.split("-")[0])) ? String.valueOf(timeForId) + "-0" : String.valueOf(timeForId) + "-" + String.valueOf(Long.parseLong(lastId.split("-")[1]) + 1);
                                            }
                                            System.out.println("Inserting streamId: " + streamId);
                                            StreamNode newStreamNode = new StreamNode(streamId, streamData);
                                            result = retrievedStream.insertNewNode(newStreamNode);
                                            System.out.println("Result of inserting " + streamId + ": " + result);
                                        }
                                        if (!result) {
                                            if (streamId.equals("0-0") || Long.parseLong(entryParts[0]) < Long.parseLong("0")) {
                                                channel.write(ByteBuffer.wrap("-ERR The ID specified in XADD must be greater than 0-0\r\n".getBytes()));
                                                break;
                                            } else {
                                                channel.write(ByteBuffer.wrap("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n".getBytes()));
                                                break;
                                            }
                                        }
                                        StringBuilder xaddResponse = new StringBuilder();
                                        xaddResponse.append("$");
                                        xaddResponse.append(streamId.length());
                                        xaddResponse.append("\r\n" + streamId + "\r\n");
                                        System.out.println("Sending XADD Response: " + xaddResponse.toString());
                                        channel.write(ByteBuffer.wrap(xaddResponse.toString().getBytes()));
                                        break;
                                    case "XRANGE":
                                        System.out.println("Received XRANGE Command");
                                        String range_start = parts[6];
                                        String range_end = parts[8];
                                        String streamKey = parts[4]; 
                         
                                        Stream streamToQuery = streamMap.get(streamKey);

                                        range_start = (range_start.equals("-")) ? "0-0" : range_start;
                                        range_end = (range_end.equals("+")) ? streamToQuery.lastID : range_end;

                                        System.out.println("Finding all nodes in the range: " + range_start + " to " + range_end);
                                        ArrayList<NodeWithBuiltPrefix> nodesInRange = streamToQuery.findInRange(range_start, range_end); 

                                        Collections.sort(nodesInRange, (a,b) -> a.prefixBuilt.compareTo(b.prefixBuilt));

                                        System.out.println("Nodes in range: ");
                                        for (NodeWithBuiltPrefix node: nodesInRange) {
                                            System.out.println(node.prefixBuilt);
                                        }
                                        StringBuilder xrangeResponse = new StringBuilder();
                                        xrangeResponse.append("*").append(nodesInRange.size()).append("\r\n");
                                        for (NodeWithBuiltPrefix node: nodesInRange) {
                                            xrangeResponse.append("*2\r\n");
                                            String retrievedPrefix = node.prefixBuilt;
                                            xrangeResponse.append("$").append(retrievedPrefix.length()).append("\r\n").append(retrievedPrefix).append("\r\n");
                                            try {
                                                System.out.println("Attempting to retrieve data at node: " + node.prefixBuilt);
                                                HashMap<String, String> dataInNode = node.node.data;
                                                int numKeyVals = 2 * dataInNode.size();
                                                xrangeResponse.append("*").append(numKeyVals).append("\r\n");
                                                for (String StreamNodeKey: dataInNode.keySet()) {
                                                    int keyLength = StreamNodeKey.length();
                                                    String StreamNodeVal = dataInNode.get(StreamNodeKey);
                                                    int valLength = StreamNodeVal.length();
                                                    xrangeResponse.append("$").append(keyLength).append("\r\n").append(StreamNodeKey).append("\r\n");
                                                    xrangeResponse.append("$").append(valLength).append("\r\n").append(StreamNodeVal).append("\r\n");
                                                }
                                            } catch (Error e) {
                                                e.printStackTrace();
                                            }
                                            
                                        }
                                        System.out.println("Response to XRANGE: ");
                                        System.out.println(xrangeResponse);
                                        channel.write(ByteBuffer.wrap(xrangeResponse.toString().getBytes())); 
                                        break;
                                    case "XREAD":
                                        System.out.println("XREAD command received");
                                        ArrayList<String> xreadStreamNames = new ArrayList<>();
                                        for (int i = 6; i < parts.length - 2; i += 2) {
                                            xreadStreamNames.add(parts[i]);
                                        }
                                        String lowBound = parts[parts.length - 1];
                                        
									default: 
					    				break;

								}
									
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
