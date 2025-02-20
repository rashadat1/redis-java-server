import java.util.HashMap;

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
        this(prefix); // call empty constructor to initialize children and prefix
        this.data = data;
    }
}

public class Stream {
    StreamNode root;

    public Stream() {
        this.root = new StreamNode("");
    }
    public void insertNode(String id, int start, int numCharConsumed, StreamNode child, StreamNode parent) {
        StreamNode intermediateNode = new StreamNode(child.prefix.substring(0,numCharConsumed)); // slice common characters with insert string
        StreamNode notInInsertNode = new StreamNode(child.prefix.substring(numCharConsumed)); // slice characters not in substring
        StreamNode newNode = new StreamNode(id.substring(start));

        parent.children.remove(child.prefix);
        parent.children.put(intermediateNode.prefix, intermediateNode);
        intermediateNode.children.put(notInInsertNode.prefix, notInInsertNode);
        intermediateNode.children.put(newNode.prefix, newNode);
        notInInsertNode.children = child.children;
    }
    public void updateNode(StreamNode entry) {
    }
    public void simpleInsertNode(StreamNode entry, StreamNode insertionPlace) {
        insertionPlace.children.put(entry.prefix, entry);
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
    public void findLongestCommonPrefix(StreamNode entry) {
        String id = entry.prefix;
        StreamNode curr = this.root;
        int start = 0; // index of end of prefix matched so far
        int end = id.length() - 1;
        if (curr.children.isEmpty()) {
            StreamNode newNode = new StreamNode(id);
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
                    insertNode(id, start, numCharConsumed, matchChildPrefixNode, curr);
                    break;
                }
                curr = matchChildPrefixNode;
            } else {
                // if we have reached a point where our remaining prefix no longer has 
                // a common element amongst the children we need to insert
                StreamNode newNode = new StreamNode(id.substring(start));
                simpleInsertNode(newNode, curr);
                break;
            }
        }
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
        stream.findLongestCommonPrefix(node1);
        stream.findLongestCommonPrefix(node2);
        stream.findLongestCommonPrefix(node3);
        stream.findLongestCommonPrefix(node4);
        stream.findLongestCommonPrefix(node5);
        stream.findLongestCommonPrefix(node6);
        stream.findLongestCommonPrefix(node7);
        stream.findLongestCommonPrefix(node8);
        stream.findLongestCommonPrefix(node9);
        stream.findLongestCommonPrefix(node10);
        stream.printTree(stream.root,"");
    }
}
