import java.nio.channels.SocketChannel;
import java.util.ArrayList;

public class XReadBlock {
    ArrayList<Stream> streamsWaitingOn;
    SocketChannel channel;
    ArrayList<String> lowBoundId;
    Long expiry;


    public XReadBlock(ArrayList<Stream> streamsWaitingOn, SocketChannel channel, ArrayList<String> lowBoundId, Long expiry) {
        this. streamsWaitingOn = streamsWaitingOn;
        this.channel = channel;
        this.lowBoundId = lowBoundId;
        this.expiry = expiry;
    }
}
