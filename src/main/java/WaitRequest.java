import java.nio.channels.SocketChannel;

public class WaitRequest {
    int numReplicas;
    long timeOut;
    ConnectionContext ctx;
    SocketChannel channel;

    public WaitRequest(int numReplicas, long timeOut, ConnectionContext ctx) {
        this.numReplicas = numReplicas;
        this.timeOut = timeOut;
        this.ctx = ctx;
    }
    public WaitRequest(int numReplicas, long timeOut, SocketChannel channel) {
        this.numReplicas = numReplicas;
        this.timeOut = timeOut;
        this.channel = channel;
    }

}
