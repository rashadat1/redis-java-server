import java.util.HashMap;
import java.util.ArrayList;
import java.util.Stack;
class StreamNode {
    String prefix;
    HashMap<String,Object> data;
    HashMap<String,StreamNode> children;
    
    // constructor for intermediate nodes which do not store data (used for lookup)
    public StreamNode(String prefix) {
        this.prefix = prefix;
        this.data = null;
        this.children = new HashMap<>();
    }
    public StreamNode(String prefix, HashMap<String,Object> data) {
        // constructor for terminal nodes - these do store data
        this(prefix); // call empty constructor to initialize children and prefi
        this.data = data;
    }
}

public class Stream {
    StreamNode root;
    String lastID;

    public Stream() {
        this.root = new StreamNode("");
        this.lastID = "0-0";
    }
    public void pushToTree(StreamNode entry, int start, int numCharConsumed, StreamNode child, StreamNode parent) {
        StreamNode intermediateNode = new StreamNode(child.prefix.substring(0,numCharConsumed)); // slice common characters with insert string
        StreamNode notInInsertNode = new StreamNode(child.prefix.substring(numCharConsumed)); // slice characters not in substring
        StreamNode newNode = new StreamNode(entry.prefix.substring(start), entry.data);

        parent.children.remove(child.prefix);
        parent.children.put(intermediateNode.prefix, intermediateNode);
        intermediateNode.children.put(notInInsertNode.prefix, notInInsertNode);
        intermediateNode.children.put(newNode.prefix, newNode);
        notInInsertNode.children = child.children;

        this.lastID = entry.prefix;
    }
    public void updateNode(StreamNode entry) {
    }
    public void simpleInsertNode(StreamNode entry, StreamNode insertionPlace) {
        insertionPlace.children.put(entry.prefix, entry);
        this.lastID = entry.prefix;
    }
    public int prefixMatchingEnd(int start, String id, StreamNode curr) {
        // traverse the current node to find the common ancestor with id to insert / search
        int n = id.length();
        int m = curr.prefix.length();
        int i = 0;
        while (start + i < n && i < m && id.charAt(start + i) == curr.prefix.charAt(i)) {
            i++;
        }
        return i;
    }
    public boolean insertNewNode(StreamNode entry) {
        String id = entry.prefix;
        StreamNode curr = this.root;
        int start = 0; // index of end of prefix matched so far
        int end = id.length() - 1;
        String[] entryParts = id.split("-");
        String[] lastIDParts = this.lastID.split("-");
        // check if entry.prefix is at least as large as the current lastID
        if (Long.parseLong(entryParts[0]) < Long.parseLong(lastIDParts[0])) {
            System.out.println("Insertion failed because first part to enter is smaller");
            System.out.println("EntryId: " + id);
            System.out.println("LastID: " + lastID);
            return false;
        } else if (Long.parseLong(entryParts[0]) == Long.parseLong(lastIDParts[0])) {
            if (Long.parseLong(entryParts[1]) <= Long.parseLong(lastIDParts[1])) {
                System.out.println("Failed because first parts to enter are the same size but second part of to enter is smaller");
                System.out.println("EntryId: " + id);
                System.out.println("LastID: " + lastID);
                return false;

            }
        }
        if (curr.children.isEmpty()) {
            StreamNode newNode = new StreamNode(id, entry.data);
            curr.children.put(id, newNode);
        }
        while (!curr.children.isEmpty()) {
            StreamNode matchChildPrefixNode = null;
            for (String prefixFragment : curr.children.keySet()) {
                // check the prefixFragments in the current node's children
                if (id.charAt(start) == prefixFragment.charAt(0)) {
                    matchChildPrefixNode = curr.children.get(prefixFragment);
                    break; // found match exit early
                }
            }
            if (matchChildPrefixNode != null) {
                // if one of the child nodes contains the next part of the prefix continue traversal
                int numCharConsumed = this.prefixMatchingEnd(start, id, matchChildPrefixNode);
                start = start + numCharConsumed;
                // need to check if we fully consumed the prefix to insert at this node
                if (numCharConsumed == matchChildPrefixNode.prefix.length()) {
                    // either we are done or we need to move on based on our current position in the node to add
                    if (start == end) {
                        // we reached the end of our insert id finding an exact match
                        break;
                    } else {
                        // we have not finished processing the id to insert so proceed because we finished the prefix at the node that matches
                        curr = matchChildPrefixNode;
                    }
                } else {
                    // split case: we did not fully consume the current node's prefix 
                    pushToTree(entry, start, numCharConsumed, matchChildPrefixNode, curr);
                    break;
                }
                curr = matchChildPrefixNode;
            } else {
                // if we have reached a point where our remaining prefix no longer has 
                // a common element amongst the children we need to insert
                StreamNode newNode = new StreamNode(id.substring(start), entry.data);
                simpleInsertNode(newNode, curr);
                break;
            }
        }
        return true;

    }
    public ArrayList<StreamNode> findInRange(String startTime, String endTime) {
        // find and return a list of streamNodes with timestamps between start and end time
        ArrayList<StreamNode> result = new ArrayList<>();
        Stack<NodeWithBuiltPrefix> stack = new Stack<>();
        NodeWithBuiltPrefix root_prefix = new NodeWithBuiltPrefix(this.root, this.root.prefix);
        stack.push(root_prefix);
        while (!stack.isEmpty()) {
            NodeWithBuiltPrefix node_prefix = stack.pop(); 
            StreamNode node = node_prefix.node;
            String prefixBuiltSoFar = node_prefix.prefixBuilt;
            for (String child_prefix : node.children.keySet()) {
                String prefixWithChild = prefixBuiltSoFar + child_prefix;
                
                if (this.withinRange(prefixWithChild, startTime, endTime)) {
                    NodeWithBuiltPrefix prefixToAdd = new NodeWithBuiltPrefix(node.children.get(child_prefix), prefixWithChild);
                    stack.push(prefixToAdd);
                }
            }
            if (node.children.isEmpty()) {
                if (this.withinRange(prefixBuiltSoFar, startTime, endTime)) {
                    result.add(node);
                }
            }
        }
        return result;



        

    }
    private boolean withinRange(String prefix, String startTime, String endTime) {
        String[] prefixParts = prefix.split("-");
        // need to pad prefixParts[0] until its the same length as startTime / end Time
        String startMilliSecTime = startTime.split("-")[0];
        String endMilliSecTime = endTime.split("-")[0];
        int entryIdLength = startMilliSecTime.length();

        if (entryIdLength != endTime.length()) {
            System.out.println("Start and end times have different lengths");
        }
        // pad prefix so far so we can check to see if we are in bounds
        // we basically do a dfs and if a node's prefix so far exceeds the bounds then we end this exploration
        String paddedPrefix = String.format("%-" + entryIdLength + "s", prefixParts[0]).replace(" ", "0");
        boolean greaterThanStart = false;
        boolean lessThanEnd = false;
        if (Long.parseLong(paddedPrefix) == Long.parseLong(startMilliSecTime)) {
            greaterThanStart = (startTime.split("-").length == 2) ? (Long.parseLong(startTime.split("-")[1]) <= Long.parseLong(prefixParts[1])) : true;
        } else {
            greaterThanStart = Long.parseLong(paddedPrefix) > Long.parseLong(startMilliSecTime);
        }
        if (Long.parseLong(paddedPrefix) == Long.parseLong(endMilliSecTime)) {
            lessThanEnd = (endTime.split("-").length == 2) ? (Long.parseLong(endTime.split("-")[1]) >= Long.parseLong(prefixParts[1])) : true;
        } else {
            lessThanEnd = Long.parseLong(paddedPrefix) < Long.parseLong(endMilliSecTime);
        }

        return (greaterThanStart && lessThanEnd);
    }
    public void printTree(StreamNode node, String indent) {
        if (node == null) {
            return;
        }
        System.out.println(indent + "└── " + node.prefix);
        for (StreamNode child : node.children.values()) {
            printTree(child, indent + "     ");
        }
    }
    public static void main(String[] args) {
        Stream stream = new Stream();
        StreamNode node1 = new StreamNode("1526919030474-0");
        StreamNode node2 = new StreamNode("1526919030474-1");
        StreamNode node3 = new StreamNode("1526919030500-0");
        StreamNode node4 = new StreamNode("9999999999999-0");
        StreamNode node5 = new StreamNode("9999999950000-0");
        StreamNode node6 = new StreamNode("1234543212341-0");
        StreamNode node7 = new StreamNode("1341243231459-0");
        StreamNode node8 = new StreamNode("1341255555555-0");
        StreamNode node9 = new StreamNode("1341255555555-1");
        StreamNode node10 = new StreamNode("1526933000000-0");
        StreamNode node11 = new StreamNode("1526933050000-0");
        StreamNode node12 = new StreamNode("1341243559990-0");
        stream.insertNewNode(node1);
        stream.insertNewNode(node2);
        stream.insertNewNode(node3);
        stream.insertNewNode(node4);
        stream.insertNewNode(node5);
        stream.insertNewNode(node6);
        stream.insertNewNode(node7);
        stream.insertNewNode(node8);
        stream.insertNewNode(node9);
        stream.insertNewNode(node10);
        stream.insertNewNode(node11);
        stream.insertNewNode(node12);
        stream.printTree(stream.root,"");
        System.out.println(stream.getClass());
        System.out.println(stream.getClass() == Stream.class);
    }
}
