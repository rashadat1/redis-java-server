import java.nio.channels.SocketChannel;
import java.util.ArrayList;

public class XReadBlock {
    ArrayList<Stream> streamsWaitingOn;
    SocketChannel channel;
    Long lowBoundId;

    public XReadBlock(ArrayList<Stream> streamsWaitingOn, SocketChannel channel, Long lowBoundId) {
        this. streamsWaitingOn = streamsWaitingOn;
        this.channel = channel;
        this.lowBoundId = lowBoundId;
    }
}
