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
    public void insertNode(StreamNode entry, StreamNode child, StreamNode parent) {

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
    public void findLongestCommonPrefix(String id) {
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
                        updateNode();
                    } else {
                        // we have not finished processing the id to insert so proceed
                        curr = matchChildPrefixNode;
                    }
                } else {
                    // split case
                    StreamNode newNode = new StreamNode(id.substring(start));
                    insertNode(newNode, matchChildPrefixNode, curr);
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
}