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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
// Build Redis
public class EventLoopServer {
	
	private static int port = 6379;
	
	public static Map<String, String> data = new ConcurrentHashMap<>();
	public static Map<String, LocalDateTime> expiryTimes = new ConcurrentHashMap<>();
	private static String dir = null;
	private static String dbfilename = null;
	private static boolean isreplica = false;
	public static String master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
	public static int master_repl_offset = 0;
	private static String master_host = null;
	private static int master_port = 0;
    private static String empty_rdb_contents = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
    private static List<SocketChannel> replicaSocketList = new CopyOnWriteArrayList<>();
	
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
				int hashTableSize = readSize(fileBuffer);
				int expiryTableSize = readSize(fileBuffer);
			
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
							clientChannel.register(selector,  SelectionKey.OP_READ, "client");
							System.out.println("New client connected: " + clientChannel.getRemoteAddress());
						} else {
							continue;
						}
					} else if (currKey.isReadable()) {
						// check if event on the channel is a READ event
						SocketChannel channel = (SocketChannel) currKey.channel();
						String sourceType = (String) currKey.attachment(); // retrieve metadata - master or client
						System.out.println("Reading from " + sourceType + " channel");
						// Read data from the client
						buffer.clear();
						// clears the byte buffer to prepare it for a new read operation
						int bytesRead = channel.read(buffer); // .read returns the number of bytes read - if no data is available returns 0, if client closed connection return -1
						if (bytesRead == -1) {
							System.out.println("Channel disconnected: " + channel.getRemoteAddress());
							currKey.cancel(); // cancel the key and remove it from the Selector
							channel.close();
							continue; // continue to the next key
						}
						buffer.flip(); 
						// prepares the ByteBuffer for reading the data it has received - set buffer limit to the current position
						// and resets the position to 0 so we can start reading at the beginning of the buffer
						String message = new String(buffer.array(), 0, bytesRead);
						// buffer.array is the byte array backing the buffer 0 and bytesRead are the starting and ending points of the read
						System.out.println("Message Received " + message);
						String[] subcommands = message.split("\\*");
						for (String subcommand : subcommands) {
							if (subcommand.isEmpty()) {
								continue;
							}
							subcommand = "*" + subcommand;
							System.out.println("Received subcommand: " + subcommand);
							String[] parts = subcommand.split("\r\n");
							
							if (parts.length >= 2) {
								String command = parts[2].toUpperCase();
								ByteBuffer responseBuffer = null;
								switch(command) {
									case "PING":
										responseBuffer = ByteBuffer.wrap("+PONG\r\n".getBytes());
										channel.write(responseBuffer);
										break;
									case "ECHO":
										System.out.println("Echo Argument: " + parts[4]);
										// parts[3] = $[length of echo argument]
										responseBuffer = ByteBuffer.wrap((parts[3] + "\r\n" + parts[4] + "\r\n").getBytes());
										channel.write(responseBuffer);
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
										    channel.write(responseBuffer);
	                                        for (SocketChannel channelreplica : replicaSocketList) {
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
										String key = data.get(parts[4]);
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
										System.out.println("REPLCONF command received");
										responseBuffer = ByteBuffer.wrap("+OK\r\n".getBytes());
										channel.write(responseBuffer);
										break;
	                                case "PSYNC":
	                                    System.out.println("PSYNC command received");
	                                    String PsyncResponse = "+FULLRESYNC " + master_replid + " " + master_repl_offset + "\r\n";
	                                    responseBuffer = ByteBuffer.wrap(PsyncResponse.toString().getBytes());
	                                    //System.out.println("RESP Simple String response to PSYNC command: " + response);
	                                    channel.write(responseBuffer);
	                                    // add all replica channels to our replica socket list
	                                    replicaSocketList.add(channel);
	                                    
	                                    // Convert raw hex string for empty RDB file contents into raw binary data (byte array)
	                                    // get the length of the byte array then wrap the content as bytes and send the length  
	                                    // followed by the contents
	                                    byte[] decodedRdb = decodeHex(empty_rdb_contents);
	                                    String rdbHeader = "$" + decodedRdb.length + "\r\n";
	                                    // Send RDB file length header
	                                    channel.write(ByteBuffer.wrap(rdbHeader.getBytes()));
	                                    // Send RDB file contents
	                                    channel.write(ByteBuffer.wrap(decodedRdb));
	                                    break;
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
