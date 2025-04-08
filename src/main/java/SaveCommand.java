import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SaveCommand implements RedisCommand {
    Long lastSaveTimestamp;
    Long nextSaveTimestamp;
    long saveInterval;
    ConcurrentHashMap<String, String> db;
    ConcurrentHashMap<String, Instant> expiryTimes;
    HashMap<String, Stream> streams;
    
    public SaveCommand(Long lastSaveTimestamp, Long nextSaveTimestamp, long saveInterval, ConcurrentHashMap<String, String> db, ConcurrentHashMap<String, Instant> expiryTimes, HashMap<String, Stream> streams) {
        this.lastSaveTimestamp = lastSaveTimestamp;
        this.nextSaveTimestamp = nextSaveTimestamp;
        this.db = db;
        this.expiryTimes = expiryTimes;
        this.streams = streams;
    }
    @Override
    public StringBuilder processCommand() {
        byte RDB_TYPE_STRING = (byte) 0x00;
        System.out.println("Creating RDB snapshot");
        try (FileOutputStream fos = new FileOutputStream("dump.rdb")) {
            FileChannel toFileChannel = fos.getChannel();
            toFileChannel.write(ByteBuffer.wrap("REDIS0011".getBytes()));
            for (Map.Entry<String,String> keyValuePair : this.db.entrySet()) {
                if (this.expiryTimes.containsKey(keyValuePair.getKey())) {
                    // this is a key-value pair with an expiration 
                    Instant expiration = this.expiryTimes.get(keyValuePair.getKey());
                    toFileChannel.write(ByteBuffer.wrap(new byte[] {(byte) 0xFC}));
                    this.writeTimeMs(toFileChannel, expiration);
                } else {
                    // key-value pair without an expiration 
                    toFileChannel.write(ByteBuffer.wrap(new byte[] {(byte) 0x00}));
                }
                this.writeString(toFileChannel, keyValuePair.getKey());
                this.writeString(toFileChannel, keyValuePair.getValue());
            }
            
        } catch (IOException e) {
            System.err.println("Error while writing to file: " + e);
        }

        return new StringBuilder();

    }
    private void writeSize(FileChannel channel, int size) throws IOException {
         // numbers up to 63 can be stored in 1 byte - we store the 6 bit length in the last 
        // 6 digits of the first byte and prefix it with 00 so: 00xxxxxx = the size we write
        if (size <= 0x3F) {
            byte b = (byte) (size & 0x3F);
            channel.write(ByteBuffer.wrap(new byte[] {b}));
        } else if (size <= 0x3FFF) {
            // numbers up to 16383 are stored in 2 bytes - the first two bits of the first byte 
            // are 01 the remaaining 14 bits are the length
            // so we store 01xxxxxx in byte 1 and xxxxxxxx in byte 2
            byte firstByte = (byte) (((size >> 8) & 0x3F) | 0x40); // shift by 8 bits so we are left with 6 bits then mask out these bits and set the first two to 01
            // now we don't shift - then we are considering the "secondByte of the size"
            byte secondByte = (byte) (size & 0xFF);
            channel.write(ByteBuffer.wrap(new byte[] {firstByte, secondByte}));
        } else {
            // for more than that - we use 10, discard the remaining 6 bits and the next 4 bytes are the length when we parse
            byte firstByte = (byte) 0x80;
            channel.write(ByteBuffer.wrap(new byte[] {firstByte}));
            ByteBuffer buffer = ByteBuffer.allocate(4);
            buffer.putInt(size);
            buffer.flip();
            channel.write(buffer);

        } 
    }
    private void writeTimeMs(FileChannel channel, Instant time) {
        System.out.println("To implement");
    }
    private void writeString(FileChannel channel, String key) {
        System.out.println("To implement");
    }
}