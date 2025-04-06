import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

public class XrangeCommand implements RedisCommand {
    String range_start;
    String range_end;
    String streamKey;
    HashMap<String, Stream> streamMap;

    public XrangeCommand(String range_start, String range_end, String streamKey, HashMap<String, Stream> streamMap) {
        this.range_start = range_start;
        this.range_end = range_end;
        this.streamKey = streamKey;
        this.streamMap = streamMap;
    }
    private void processStreamId() {
        this.range_start = (this.range_start.equals("-") ? "0-0" : this.range_start);
        this.range_end = (this.range_end.equals("+") ? this.streamMap.get(this.streamKey).lastID : this.range_end);

    }
    @Override
    public StringBuilder processCommand() {
        Stream streamToQuery = this.streamMap.get(this.streamKey);
        if (streamToQuery == null) {
            return new StringBuilder("Invalid stream for xrange - stream does not exist");
        }
        this.processStreamId();

        ArrayList<NodeWithBuiltPrefix> nodesInRange = streamToQuery.findInRange(this.range_start, this.range_end);
        Collections.sort(nodesInRange, (a,b) -> a.prefixBuilt.compareTo(b.prefixBuilt)); // sort for ascending order
        StringBuilder xrangeResponse = new StringBuilder();
        xrangeResponse.append("*").append(nodesInRange.size()).append("\r\n");
        for (NodeWithBuiltPrefix node: nodesInRange) {
            xrangeResponse.append("*2\r\n");
            String retrievedPrefix = node.prefixBuilt;
            xrangeResponse.append("$").append(retrievedPrefix.length()).append("\r\n").append(retrievedPrefix).append("\r\n");
            try {
                System.out.println("Attempting to retrieve data at node: " + node.prefixBuilt);
                HashMap<String, String> dataInNode = node.node.data;
                int numKeyVals = 2 * dataInNode.size();
                xrangeResponse.append("*").append(numKeyVals).append("\r\n");
                for (String StreamNodeKey: dataInNode.keySet()) {
                    int keyLength = StreamNodeKey.length();
                    String StreamNodeVal = dataInNode.get(StreamNodeKey);
                    int valLength = StreamNodeVal.length();
                    xrangeResponse.append("$").append(keyLength).append("\r\n").append(StreamNodeKey).append("\r\n");
                    xrangeResponse.append("$").append(valLength).append("\r\n").append(StreamNodeVal).append("\r\n");
                }
            } catch (Error e) {
                e.printStackTrace();
            }   
        }
        return xrangeResponse;
    }  
}
