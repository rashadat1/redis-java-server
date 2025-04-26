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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
// Build Redis

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
public class EventLoopServerSecure {
	
	private static int port = 6379;
	public static ConcurrentHashMap<String, String> data = new ConcurrentHashMap<>();
	public static ConcurrentHashMap<String, Instant> expiryTimes = new ConcurrentHashMap<>();
	private static String dir = null;
	private static String dbfilename = null;
	private static boolean isreplica = false;
	public static String master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
	public static int master_repl_offset = 0;
	private static String master_host = null;
	private static int master_port = 0;
    final static String empty_rdb_contents = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
    final static List<ConnectionContext> replicaConnectionContextList = new CopyOnWriteArrayList<>();
	private static int replConfOffset = 0;
    private static int acknowledgedReplicas = 0;
    private static int writeOffset = 0;
    private static WaitRequest waitRequest = null;
    private static ArrayList<XReadBlock> listBlockingXReads = new ArrayList<>();
    private static ArrayList<XReadBlock> listNoTimeoutXReads = new ArrayList<>();
    private static HashMap<SocketChannel, Queue<RedisCommand>> pendingTransactionQueues = new HashMap<>();
    private static ArrayList<SaveCommand> saveCommandSchedule = new ArrayList<>();
	private static String keystorePassword;
	private static String trustStorePassword;
	private static SSLContext sslContext = SSLUtil.createSSLContext(keystorePassword, trustStorePassword);
	// if prod flag is true we require any connection be authenticated
	private static ArrayList<SocketChannel> authenticatedSockets = new ArrayList<>();

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
				Instant expirationTime = Instant.ofEpochSecond(expiryTime);
				
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
				Instant expirationTime = Instant.ofEpochMilli(expiryTimeMillis);
				
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
	private static void doHandshakeStep(ConnectionContext ctx) throws IOException {
		SSLEngineResult result;
		SSLEngineResult.HandshakeStatus handshakeStatus = ctx.sslEngine.getHandshakeStatus();

		while (true) {
			switch (handshakeStatus) {
				case NEED_UNWRAP:
					int bytesRead = ctx.channel.read(ctx.peerNetData); // read bytes from channel into peerNetData buffer
					if (bytesRead == -1) {
						throw new IOException("Channel closed before handshake");
					}
					ctx.peerNetData.flip(); // flip for reading
					result = ctx.sslEngine.unwrap(ctx.peerNetData, ctx.peerAppData);
					ctx.peerNetData.compact();
					handshakeStatus = result.getHandshakeStatus();
					break;

				case NEED_WRAP:
				case NEED_UNWRAP_AGAIN:
					ctx.netData.clear();
					result = ctx.sslEngine.wrap(ctx.appData, ctx.netData);
					ctx.netData.flip();
					while (ctx.netData.hasRemaining()) {
						ctx.channel.write(ctx.netData);
					}
					handshakeStatus = result.getHandshakeStatus();
					break;

				case NEED_TASK:
					Runnable task;
					while ((task = ctx.sslEngine.getDelegatedTask()) != null) {
						task.run();
					}
					handshakeStatus = ctx.sslEngine.getHandshakeStatus();
					break;

				case FINISHED:
				case NOT_HANDSHAKING:
					ctx.handshaking = false;
					System.out.println("Handshake complete for connection: " + ctx.channel.getRemoteAddress());
					return;
			}
			if (handshakeStatus == SSLEngineResult.HandshakeStatus.NEED_UNWRAP && ctx.peerNetData.position() == 0) {
				break;
			}
		}
	}
	private static void encryptAndSendResponse(ConnectionContext ctx, String response) throws IOException {
		ctx.appData.clear();
		ctx.netData.clear();

		ctx.appData.put(response.getBytes());
		ctx.appData.flip();
		ctx.sslEngine.wrap(ctx.appData, ctx.netData);

		ctx.netData.flip();
		while (ctx.netData.hasRemaining()) {
			ctx.channel.write(ctx.netData);
		}
		ctx.appData.clear();
		ctx.netData.clear();
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
                ArrayList<Long> xReadBlockTime = new ArrayList<>();
                if (waitRequest != null) {
                    // check if we are processing a wait command - if so we should check to see if the timeout has been reached
                    blockTime = waitRequest.timeOut - currentTime;
                    if (blockTime <= 0) {
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
                List<XReadBlock> toRemove = new ArrayList<>();
                if (listBlockingXReads.size() != 0) {
                    System.out.println("Blocking Xreads occurred, checking if any expired...");
                    // check if we have any blocking xreads in waiting
                    for (XReadBlock xreadblock : listBlockingXReads) {
                        // for each of these blocking xreads add the remaining blocking time to an array of longs 
                        xReadBlockTime.add(xreadblock.expiry - currentTime);
                        if (currentTime >= xreadblock.expiry) {
                            // and check if we have passed the time 
                            System.out.println("Found expired blocking Xread!");
                            System.out.println("XREAD time out reached for xread waiting on streams: " + xreadblock.streamsWaitingOn + " with expiry " + xreadblock.expiry);
                            toRemove.add(xreadblock);

                            XreadCommand xreadCommand = new XreadCommand(null, streamMap, xreadblock.streamsWaitingOn, xreadblock.lowBoundId, 0, false);
                            StringBuilder xreadResult = xreadCommand.processCommand();

                            System.out.println("The result of the blocked xread is: " + xreadResult.toString());
                            if (xreadResult.toString().equals("$-1\r\n")) {
                                // no xadd occurred in streams waiting on so send bulk empty string 
                                xreadblock.channel.write(ByteBuffer.wrap(xreadResult.toString().getBytes()));
                            } else {
                                // xadd did occur in streams waiting!
                                System.out.println("xaddds occurred while channels blocked!");
                                System.out.println(xreadResult);
                                StringBuilder starter = new StringBuilder("*").append(xreadCommand.numNodesWithDataRead).append("\r\n");
                                xreadblock.channel.write(ByteBuffer.wrap((starter.toString() + xreadResult.toString()).getBytes()));
                            }
                        }
                    }
                    listBlockingXReads.removeAll(toRemove);
                }
                long largestXreadTimeout = xReadBlockTime.isEmpty() ? 0 : Collections.max(xReadBlockTime);
				// Select ready channels using the selector
                if (listNoTimeoutXReads.size() != 0) {
                    // if there are - no timeout xreads - block indefinitely
                    selector.select(0);
                } else {
                    // if there are no - no timeout xreads block like this:
				    selector.select(Math.max(Math.max(blockTime, 0), largestXreadTimeout)); // if we have passed the timeOut deadline then continue as normal  
                }
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
							SSLEngine sslEngine = sslContext.createSSLEngine();
							sslEngine.setUseClientMode(false);
							sslEngine.setNeedClientAuth(true);

							sslEngine.beginHandshake();
							ConnectionContext context = new ConnectionContext(clientChannel, sslEngine, "client");
							// Register for read events
							clientChannel.register(selector,  SelectionKey.OP_READ, context);
							System.out.println("New client connected: " + clientChannel.getRemoteAddress());
						} else {
							continue;
						}
					} else if (currKey.isReadable()) {
						// check if event on the channel is a READ event
						SocketChannel channel = (SocketChannel) currKey.channel();
						ConnectionContext ctx = (ConnectionContext) currKey.attachment(); // retrieve metadata - master or client
						String sourceType = ctx.entity;
						System.out.println("Reading from " + sourceType + " channel" + channel.getRemoteAddress());
						
						if (ctx.handshaking) {
							doHandshakeStep(ctx);
							break;
						}
						// clears the byte buffer to prepare it for a new read operation
						ctx.peerNetData.clear(); // read into the encrypted input buffer
						int bytesRead = ctx.channel.read(ctx.peerNetData);
						if (bytesRead == -1) {
							System.out.println("Channel disconnected: " + ctx.channel.getRemoteAddress());
							currKey.cancel(); // cancel the key and remove it from the Selector
							ctx.channel.close();
							continue; // continue to the next key
						}
						ctx.peerNetData.flip();
                        System.out.println(sourceType + " wrote " + bytesRead + " bytes");
						// Unwrap the encrypted bytes into plain text
						SSLEngineResult result = ctx.sslEngine.unwrap(ctx.peerNetData, ctx.peerAppData);
						
						ctx.peerNetData.compact();
						ctx.peerAppData.flip(); // flip to read the plain text
						
						// creates a byte array 
						byte[] plainText = new byte[ctx.peerAppData.remaining()];
						ctx.peerAppData.get(plainText);
						String message = new String(plainText);
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
                                        if (!isreplica) {
											encryptAndSendResponse(ctx, pingResponse);
                                            master_repl_offset += pingResponse.getBytes().length;
                                        }
										break;

									case "ECHO":
										System.out.println("Echo Argument: " + parts[4]);
                                        String echoResponse = parts[3] + "\r\n" + parts[4] + "\r\n"; 
										responseBuffer = ByteBuffer.wrap(echoResponse.getBytes());
                                        if (!isreplica) {
										    encryptAndSendResponse(ctx, echoResponse);
                                            master_repl_offset += echoResponse.getBytes().length;
                                        }
										break;

									case "SET":
										System.out.println("Set Command key: " + parts[4] + "\r\nSet Command value: " + parts[6]);
                                        SetCommand setCommand; 
                                        if (parts.length == 11 && parts[8].equalsIgnoreCase("PX")) {
                                            // set command with expiration
                                        System.out.println("Creating set Command object with expiry"); 
                                            setCommand = new SetCommand(parts[4], parts[6], parts[10], data, expiryTimes);
                                        } else {
                                            // set command without expiration
                                            System.out.println("Creating set Command object without expiry"); 
                                            setCommand = new SetCommand(parts[4], parts[6], "none", data, expiryTimes);
                                        }
                                        StringBuilder setResponse = new StringBuilder();
                                        if (pendingTransactionQueues.containsKey(channel)) {
                                            pendingTransactionQueues.get(channel).add(setCommand);
                                            setResponse.append("+QUEUED\r\n");
                                        }
                                        else {
                                            setResponse.append(setCommand.processCommand());
                                        }
	                                    if (!isreplica) {
	                                        // if this is the master we send the "+OK" response back to the client making the request
	                                        // in addition we propagate the request to the replicas in the list
                                            writeOffset += 1;
											encryptAndSendResponse(ctx, setResponse.toString());
                                            master_repl_offset += message.getBytes().length;
	                                        for (ConnectionContext replicaContext : replicaConnectionContextList) {
                                                System.out.println("Propagating Write to replica: " + replicaContext.channel.getRemoteAddress());
												encryptAndSendResponse(replicaContext, message);
	                                        }
	                                    }
										break;

									case "GET":
                                        System.out.println("Get Command received");
                                        GetCommand getCommand = new GetCommand(parts[4], data, expiryTimes);
                                        StringBuilder getResponse = new StringBuilder();
                                        if (pendingTransactionQueues.containsKey(channel)) {
                                            pendingTransactionQueues.get(channel).add(getCommand);
                                            getResponse.append("+QUEUED\r\n");
                                        } else {
                                            getResponse.append(getCommand.processCommand());
                                        } 
                                        encryptAndSendResponse(ctx, getResponse.toString());
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
										String configResponse = prefix + commandStr + "$" + paramValue.length() + "\r\n" + paramValue + "\r\n";
										encryptAndSendResponse(ctx, configResponse);
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
										encryptAndSendResponse(ctx, KeysResponse.toString());
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
											encryptAndSendResponse(ctx, InfoResponse.toString());
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
											encryptAndSendResponse(ctx, response.toString());
                                            System.out.println("Adding REPLCONF GETACK Bytes: " + subcommand.getBytes().length);
                                            replConfOffset += subcommand.getBytes().length;
                                        
                                        } else if (parts.length >= 4 && parts[4].equalsIgnoreCase("ACK")) {
                                            System.out.println("ACK received from replica");
                                            parsedREPLACK++;
                                            System.out.println(parsedREPLACK + "th ACK received out of " + replicaConnectionContextList.size() + " total replicas connected");
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
                                                if (acknowledgedReplicas >= waitRequest.numReplicas || System.currentTimeMillis() >= waitRequest.timeOut || parsedREPLACK == replicaConnectionContextList.size()) {
                                                    System.out.println("Wait command completed returning: " + ":" + acknowledgedReplicas + "\r\n");
                                                    String waitRequestResponse = ":" + acknowledgedReplicas + "\r\n";
													encryptAndSendResponse(waitRequest.ctx, waitRequestResponse);
                                                    waitRequest = null;
                                                    master_repl_offset += "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n1\r\n*\r\n".getBytes().length;
                                                    writeOffset = 0;
                                                    acknowledgedReplicas = 0;
                                                }
                                            }
                                            break;
                                            
                                        } else if (!isreplica){
                                            System.out.println("REPLCONF command received");
                                            String response = "+OK\r\n";
                                            // replica only responds to REPLCONF GETACKs from the master. The master will receive plain REPLCONF from replica during handshake
                                            encryptAndSendResponse(ctx, response);
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
	                                    encryptAndSendResponse(ctx, PsyncResponse);
	                           
	                                    // Convert raw hex string for empty RDB file contents into raw binary data (byte array)
	                                    // get the length of the byte array then wrap the content as bytes and send the length  
	                                    // followed by the contents
	                                    byte[] decodedRdb = decodeHex(empty_rdb_contents);
	                                    String rdbHeader = "$" + decodedRdb.length + "\r\n";
	                                    // Send RDB file length header
										encryptAndSendResponse(ctx, rdbHeader);
	                                    // Send RDB file contents
										encryptAndSendResponse(ctx, new String(decodedRdb));
	                                    System.out.println("Added Replica to list: " + ctx.channel.getRemoteAddress());
                                        // register the replica channel with the Selector
                                        ctx.channel.configureBlocking(false);
                                        ctx.channel.register(selector, SelectionKey.OP_READ, "replica");
                                        System.out.println("Replica registered with selector for OP_READ: " + ctx.channel.getRemoteAddress());
                                    	replicaConnectionContextList.add(ctx);
	                                    break;

                                    case "WAIT":
                                        System.out.println("WAIT command received");
                                        System.out.println("There are " + replicaConnectionContextList.size() + " total replicas");
                                        if (replicaConnectionContextList.size() == 0) {
                                            // if no replicas return immediately
											encryptAndSendResponse(ctx, ":0\r\n");
                                            break;
                                        }
                                        int numReplicas = Integer.parseInt(parts[4]);
                                        long timeOut = Long.parseLong(parts[6]);
                                        long expiry = System.currentTimeMillis() + timeOut;
                                        // store WAIT command details
                                        System.out.println("Creating WaitRequest instance - " + "numReplicas: " + numReplicas + " expiry: " + expiry);
                                        waitRequest = new WaitRequest(numReplicas, expiry, ctx);
                                        if (writeOffset == 0) {
											encryptAndSendResponse(ctx, ":" + replicaConnectionContextList.size() + "\r\n");
                                            break;
                                        }
                                        String replMessage = "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n";

                                        for (ConnectionContext replicaContext : replicaConnectionContextList) {
                                            // Send REPLCONF GETACK to all replicas
											encryptAndSendResponse(replicaContext, replMessage);
                                            System.out.println("Sent REPLCONF GETACK to replica: " + replicaContext.channel.getRemoteAddress());
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
                                        encryptAndSendResponse(ctx, typeResponseString);
                                        break;

                                    case "XADD":
                                        System.out.println("XADD command received");
                                        String streamName = parts[4];
                                        String streamId = parts[6];
                                        
                                        HashMap <String, String> streamData = new HashMap<>();
                                        for (int i = 8; i + 2 < parts.length; i += 4) {
                                            String streamKey = parts[i];
                                            String streamVal = parts[i + 2];
                                            streamData.put(streamKey, streamVal);
                                        }
                                        XaddCommand xaddCommand = new XaddCommand(streamName, streamId, streamMap, streamData);
                                        StringBuilder xaddResponse = new StringBuilder();
                                        if (pendingTransactionQueues.containsKey(channel)) {
                                            pendingTransactionQueues.get(channel).add(xaddCommand);
                                            xaddResponse.append("+QUEUED\r\n");
                                            
                                        } else {
                                            xaddResponse.append(xaddCommand.processCommand());
                                        }
										encryptAndSendResponse(ctx, xaddResponse.toString());
                                        // after sending XADD we should try to resolve the blocking Xreads
                                        XaddCommand resolveBlockXreads = new XaddCommand(null, null, streamMap, null);
                                        resolveBlockXreads.propagateToPendingXreads(listBlockingXReads, listNoTimeoutXReads);
                                        break;

                                    case "XRANGE":
                                        System.out.println("XRANGE command received");
                                        String range_start = parts[6];
                                        String range_end = parts[8];
                                        String streamKey = parts[4]; 
                         
                                        XrangeCommand xrangeCommand = new XrangeCommand(range_start, range_end, streamKey, streamMap);
                                        StringBuilder xrangeResponse = xrangeCommand.processCommand();
                                        encryptAndSendResponse(ctx, xrangeResponse.toString());
                                        break;

                                    case "XREAD":
                                        System.out.println("XREAD command received");

                                        XreadCommand xreadCommand = new XreadCommand(parts, streamMap, new ArrayList<String>(), new ArrayList<String>(), 0, false);
                                        StringBuilder xreadContent = xreadCommand.processCommand(); 

                                        StringBuilder xreadResponse = new StringBuilder("*").append(xreadCommand.numNodesWithDataRead).append("\r\n"); 
                                        if (!xreadContent.toString().equals("$-1\r\n")) {
                                            System.out.println("Xread response obtained (non-nil)");
                                            System.out.println("xreadResponse:");
                                            System.out.println(xreadContent);
											
											encryptAndSendResponse(ctx, xreadResponse.toString() + xreadContent.toString());
                                            break;
                                        } else {
                                            if (xreadCommand.blockingXread) {
                                                List<Stream> Streams = xreadCommand.xreadStreamNames.stream()
                                                    .map(streamMap::get)
                                                    .collect(Collectors.toList());
                                                Long xReadExpiry = System.currentTimeMillis() + Long.parseLong(parts[6]);
                                                XReadBlock xreadBlockObject = new XReadBlock(xreadCommand.xreadStreamNames, ctx, xreadCommand.lowBounds, xReadExpiry);
                                                if (Long.parseLong(parts[6]) == Long.parseLong("0")) {
                                                    // if timeout is 0 then it is blocking without timeout
                                                    System.out.println("Found xread with timeout 0");
                                                    listNoTimeoutXReads.add(xreadBlockObject);
                                                } else {
                                                    // otherwise add it the list of xreads that are blocking with timeout
                                                    listBlockingXReads.add(xreadBlockObject);
                                                }
                                                System.out.println("Registered block xread object with expiry: " + xReadExpiry);
                                                break;
                                            } else {
                                                // no results and not a blockingXread
                                                System.out.println("No results and not a blocking xread - returning immediately");
												encryptAndSendResponse(ctx, xreadContent.toString());
                                                break;
                                            }
                                        } 
                                    case "INCR":
                                        // increment the value associated with the received key by 1 
                                        // if the key does not exist in the hashmap - we set the value to 1
                                        System.out.println("INCR command received");
                                        StringBuilder incrResponse = new StringBuilder();
                                        IncrCommand incrCommand = new IncrCommand(parts[4], data);
                                        if (pendingTransactionQueues.containsKey(channel)) {
                                            System.out.println("Active MULTI command in channel");
                                            pendingTransactionQueues.get(channel).add(incrCommand);
                                            incrResponse.append("+QUEUED\r\n");

                                        } else {
                                            incrResponse.append(incrCommand.processCommand());
                                        } 
                                        System.out.println("IncrResponse: ");
                                        System.out.println(incrResponse);
                                        channel.write(ByteBuffer.wrap(incrResponse.toString().getBytes()));
                                        break;
                                
                                    case "MULTI":
                                        System.out.println("MULTI command received");
                                        if (pendingTransactionQueues.containsKey(channel)) {
                                            System.out.println("Pending transaction already in progress for this channel");
                                            break;
                                        }
                                        Queue<RedisCommand> transactionQueue = new ArrayDeque<>();
                                        pendingTransactionQueues.put(channel, transactionQueue); 
                                        channel.write(ByteBuffer.wrap("+OK\r\n".getBytes()));
                                        break;

                                    case "EXEC":
                                        System.out.println("EXEC command received");
                                        if (!pendingTransactionQueues.containsKey(channel)) {
                                            // EXEC command received but no transaction queue created
                                            System.out.println("EXEC command received without MULTI");
                                            channel.write(ByteBuffer.wrap("-ERR EXEC without MULTI\r\n".getBytes()));
                                            break;
                                        }
                                        if (pendingTransactionQueues.get(channel).size() == 0) {
                                            // EXEC command received, transaction queue created but no commands have been queued
                                            System.out.println("EXEC received with no queued commands");
                                            channel.write(ByteBuffer.wrap("*0\r\n".getBytes()));
                                            pendingTransactionQueues.remove(channel);
                                            break;
                                        } else {
                                            // EXEC command received, transaction queue created, and commands exist in the queued
                                            System.out.println("EXEC command received, executing queued commands");
                                            Queue<RedisCommand> queuedTransactions = pendingTransactionQueues.get(channel);
                                            StringBuilder execQueueResponse = new StringBuilder("*").append(queuedTransactions.size()).append("\r\n");
                                            while (!queuedTransactions.isEmpty()) {
                                                RedisCommand redisCommand = queuedTransactions.poll();
                                                System.out.println("Executing redis command" + redisCommand);
                                                StringBuilder responseToCommand = redisCommand.processCommand();
                                                execQueueResponse.append(responseToCommand);
                                            }
                                            pendingTransactionQueues.remove(channel);
                                            channel.write(ByteBuffer.wrap(execQueueResponse.toString().getBytes()));
                                            break;
                                        }
                                    case "DISCARD":
                                        System.out.println("Discard command received");
                                        if (pendingTransactionQueues.containsKey(channel)) {
                                            pendingTransactionQueues.remove(channel);
                                            channel.write(ByteBuffer.wrap("+OK\r\n".getBytes()));
                                            break;
                                        }
                                        channel.write(ByteBuffer.wrap("-ERR DISCARD without MULTI\r\n".getBytes()));
                                        break;
                                    case "BGSAVE":
                                        System.out.println("BGSAVE command received");
                                        SaveCommand saveCommand = new SaveCommand(null, System.currentTimeMillis(), Long.parseLong("50"), data, expiryTimes, streamMap); 
                                        if (saveCommandSchedule.size() == 1) {
                                            saveCommandSchedule.remove(0);
                                        }
                                        saveCommandSchedule.add(saveCommand);
                                        break;
									default: 
					    				break;

								}
								ctx.peerAppData.clear();
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
