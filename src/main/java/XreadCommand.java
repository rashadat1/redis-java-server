import java.util.ArrayList;
import java.util.HashMap;

public class XreadCommand implements RedisCommand {
    String[] commandParts;
    HashMap<String, Stream> streamMap;
    ArrayList<String> xreadStreamNames;
    ArrayList<String> lowBounds;
    int numNodesWithDataRead;
    boolean blockingXread;
    
    public XreadCommand(String[] commandParts, HashMap<String, Stream> streamMap, ArrayList<String> xreadStreamNames, ArrayList<String> lowBounds, int numNodesWithDataRead, boolean blockingXread) {
        this.commandParts = commandParts;
        this.streamMap = streamMap;
        this.xreadStreamNames = xreadStreamNames;
        this.lowBounds = lowBounds;
        this.numNodesWithDataRead = numNodesWithDataRead;
        this.blockingXread = blockingXread;
    }
    private void processCommandParts() {
        this.blockingXread = this.commandParts[4].equalsIgnoreCase("block");
        int blockParserOffset = this.blockingXread ? 4 : 0;
        int numPartsContent = Integer.parseInt(this.commandParts[0].replace("*","")) - 2;
        if (this.blockingXread) {
            numPartsContent -= 2;
        }
        int numStreamsQuery = numPartsContent / 2;
        for (int i = 6 + blockParserOffset; i <= this.commandParts.length - (2 * numStreamsQuery); i += 2) {
            this.xreadStreamNames.add(this.commandParts[i]);
            if (this.commandParts[i + (2 * numStreamsQuery)].equals("$")) {
                Stream streamFromMap = this.streamMap.get(this.commandParts[i]);
                String currStreamLastId = streamFromMap.lastID;
                this.lowBounds.add(currStreamLastId);
            } else {
                this.lowBounds.add(this.commandParts[i + (2 * numStreamsQuery)]);
            }
        }
    }
    @Override
    public StringBuilder processCommand() {
        if (commandParts != null) {
            // if we provided the RESP command then parse this to obtain blocking, 
            // length of command, and the input streams and lower bounds for the XRead
            this.processCommandParts();
        }
        // we only call this without commandParts to process a XReadBlock which gives us
        // the data we need as object data members
        StringBuilder xreadResponse = new StringBuilder();
        ArrayList<NodeWithBuiltPrefix> xreadRetrievedNodes;
        this.numNodesWithDataRead = 0;
        for (int i = 0; i < this.xreadStreamNames.size(); i++) {
            Stream xreadStream = this.streamMap.get(this.xreadStreamNames.get(i));
            xreadRetrievedNodes = xreadStream.readAboveBound(lowBounds.get(i));
            if (!xreadRetrievedNodes.isEmpty()) {
                this.numNodesWithDataRead += 1;
                xreadResponse.append("*2\r\n");
                xreadResponse.append("$").append(this.xreadStreamNames.get(i).length()).append("\r\n").append(this.xreadStreamNames.get(i)).append("\r\n");
                xreadResponse.append("*").append(xreadRetrievedNodes.size()).append("\r\n");
                xreadResponse.append("*2\r\n");
                for (NodeWithBuiltPrefix xreadRetrievedNode : xreadRetrievedNodes) {

                    String streamIdRetrieved = xreadRetrievedNode.prefixBuilt;
                    HashMap<String, String> dataInNode = xreadRetrievedNode.node.data;
                    
                    xreadResponse.append("$").append(streamIdRetrieved.length()).append("\r\n").append(streamIdRetrieved).append("\r\n");
                    int numKeyVals = 2 * dataInNode.size();
                    xreadResponse.append("*").append(numKeyVals).append("\r\n");
                    for (String nodeKey : dataInNode.keySet()) {
                        String nodeValFromKey = dataInNode.get(nodeKey);
                        xreadResponse.append("$").append(nodeKey.length()).append("\r\n").append(nodeKey).append("\r\n");
                        xreadResponse.append("$").append(nodeValFromKey.length()).append("\r\n").append(nodeValFromKey).append("\r\n");
                    }
                }
            }
        }
        if (this.numNodesWithDataRead > 0) {
            return xreadResponse; 
        } else {
            return new StringBuilder("$-1\r\n");
        }
    }
}
