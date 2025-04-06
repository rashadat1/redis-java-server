import java.util.concurrent.ConcurrentHashMap;

public class IncrCommand implements RedisCommand {
    String keyToIncr;
    ConcurrentHashMap<String, String> db;

    public IncrCommand(String keyToIncr, ConcurrentHashMap<String, String> db) {
        this.keyToIncr = keyToIncr;
        this.db = db;
    }
    @Override
    public StringBuilder processCommand() {
        String valToIncr = this.db.getOrDefault(keyToIncr, "0");
        try {
            String newVal = String.valueOf(Long.parseLong(valToIncr) + 1);
            this.db.put(this.keyToIncr, newVal);
            return new StringBuilder(":").append(newVal).append("\r\n");
        } catch (NumberFormatException e) {
            return new StringBuilder("-ERR value is not an integer or out of range\r\n");
        }
    }
}
