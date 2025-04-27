import java.nio.channels.SocketChannel;
import java.util.ArrayList;

public class XReadBlock {
    ArrayList<String> streamsWaitingOn;
    SocketChannel channel;
    ArrayList<String> lowBoundId;
    Long expiry;
    ConnectionContext ctx;

    public XReadBlock(ArrayList<String> streamsWaitingOn, SocketChannel channel, ArrayList<String> lowBoundId, Long expiry) {
        this. streamsWaitingOn = streamsWaitingOn;
        this.channel = channel;
        this.lowBoundId = lowBoundId;
        this.expiry = expiry;
        this.ctx = null;
    }
    public XReadBlock(ArrayList<String> streamsWaitingOn, ConnectionContext ctx, ArrayList<String> lowBoundId, Long expiry) {
        this. streamsWaitingOn = streamsWaitingOn;
        this.channel = null;
        this.lowBoundId = lowBoundId;
        this.expiry = expiry;
        this.ctx = ctx;
    }
}
