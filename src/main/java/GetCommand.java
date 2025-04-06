import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;

public class GetCommand implements RedisCommand {
    String key;
    ConcurrentHashMap<String, String> db;
    ConcurrentHashMap<String, Instant> expiryTimes;

    public GetCommand(String key, ConcurrentHashMap<String, String> db, ConcurrentHashMap<String, Instant> expiryTimes) {
        this.key = key;
        this.db = db;
        this.expiryTimes = expiryTimes;
    }
    @Override
    public StringBuilder processCommand() {
        Instant keyExpiration = this.expiryTimes.getOrDefault(this.key, null);
        if (keyExpiration != null) {
            // this means the key has an expiration 
            if (Instant.now().isAfter(keyExpiration)) {
                // expiration has passed
                return new StringBuilder("$-1\r\n");
            }
        }
        String val = db.get(this.key);
        if (val != null) {
            return new StringBuilder("$").append(val.length()).append("\r\n").append(val).append("\r\n");
        } else {
            return new StringBuilder("$-1\r\n"); 
        }
    }
}
