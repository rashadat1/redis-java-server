import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;

public class SetCommand extends RedisCommand {
    String key;
    String value;
    String expiry;
    ConcurrentHashMap<String, String> db;
    ConcurrentHashMap<String, Instant> expiryTimes; 

    public SetCommand(String key, String value, String expiry, ConcurrentHashMap<String, String> db, ConcurrentHashMap<String, Instant> expiryTimes) {
        this.key = key;
        this.value = value;
        this.expiry = expiry;
        this.db = db;
        this.expiryTimes = expiryTimes;
    }
    @Override
    public StringBuilder processCommand() {
        this.db.put(this.key, this.value);
        if (this.expiry == null) {
            // set command without PX
            this.expiryTimes.remove(this.key);
        } else {
            // set command with PX
            long expiryTimeMillis = 1_000_000 * Long.parseLong(this.expiry);
            Instant expiryTime = Instant.now().plusNanos(expiryTimeMillis);
            this.expiryTimes.put(this.key, expiryTime);
        }
        return new StringBuilder("+OK\r\n");
    }
}
