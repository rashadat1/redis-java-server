import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSession;

public class ConnectionContext {
    SocketChannel channel;
    SSLEngine sslEngine;
    String entity;

    ByteBuffer peerNetData; // encrypted bytes received from channel
    ByteBuffer peerAppData; // decrypted bytes after unwrap

    ByteBuffer netData; // encrypted bytes to send to channel
    ByteBuffer appData; // plain bytes to encrypt and send

    boolean handshaking;
    Boolean finishedMasterReplHandshake;
    public ConnectionContext(SocketChannel channel, SSLEngine sslEngine, String entity) {
        this.channel = channel;
        this.sslEngine = sslEngine;
        this.entity = entity;
        this.handshaking = true;
        if (this.entity.equals("master")) {
            this.finishedMasterReplHandshake = false;
        } else {
            this.finishedMasterReplHandshake = null;
        }

        SSLSession session = sslEngine.getSession();
        this.peerNetData = ByteBuffer.allocate(session.getPacketBufferSize());
        this.peerAppData = ByteBuffer.allocate(session.getApplicationBufferSize());
        this.netData = ByteBuffer.allocate(session.getPacketBufferSize());
        this.appData = ByteBuffer.allocate(session.getApplicationBufferSize());
    }
}