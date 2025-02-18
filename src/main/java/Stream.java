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
    public void insertNode(StreamNode entry, StreamNode insertionPlace) {

    }
    public void updateNode(StreamNode entry) {
    }
    public int prefixMatchingEnd(int start, String id, StreamNode curr) {
        // traverse the current node to find the common ancestor with id to insert / search
        int n = id.length();
        int m = curr.prefix.length();
        int i = 0;
        while (start + i < n && i < m && id.charAt(start + i) == curr.prefix.charAt(i)) {
            i++;
        }
        return start + i;
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
                // continue to traverse the tree
                start = this.prefixMatchingEnd(start, id, matchChildPrefixNode);
                curr = matchChildPrefixNode;
            } else {
                // if we have reached a point where our remaining prefix no longer has a common element
                // need to insert here - two insert scenarios either the current node has no children or it has at least one
                StreamNode newNode = new StreamNode(id);
                insertNode(newNode);
                break;
            }
            if (start == end) {
                // update existing node
                updateNode();
            }

// 1713296 to insert
// 1713 has children 12345
// match at 2 which has 
        }
    }
}
