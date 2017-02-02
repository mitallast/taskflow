package org.github.mitallast.taskflow.common.path;

import org.github.mitallast.taskflow.common.strings.QueryStringDecoder;
import org.github.mitallast.taskflow.common.strings.Strings;

import java.util.Arrays;
import java.util.Map;

@SuppressWarnings("unchecked")
public class PathTrie<TrieType> {

    private final static char separator = '/';
    private final TrieNode<TrieType> root;

    public PathTrie() {
        root = new TrieNode<>(String.valueOf(separator), null);
    }

    public void insert(String path, TrieType value) {
        String[] parts = Strings.splitStringToArray(path, separator);
        if (parts.length == 0) {
            root.value = value;
            return;
        }
        int index = 0;
        // supports initial delimiter.
        if (parts.length > 0 && parts[0].length() == 0) {
            index = 1;
        }
        root.insert(parts, index, value);
    }

    public TrieType retrieve(String path, Map<String, String> params) {
        if (path.length() == 0 || path.length() == 1 && path.charAt(0) == separator) {
            return root.value;
        }
        int index = 0;
        if (path.charAt(0) == separator) {
            index = 1;
        }
        return root.retrieve(path, index, params);
    }

    public void prettyPrint() {
        root.prettyPrint(0, "", true);
    }

    private static class TrieNode<NodeType> {
        private final static TrieNode[] empty = new TrieNode[0];

        private final String key;
        private final String namedWildcard;
        private TrieNode<NodeType>[] children;
        private TrieNode<NodeType>[] childrenNamedWildcard;
        private NodeType value;

        public TrieNode(String key, NodeType value) {
            this.value = value;
            this.children = empty;
            this.childrenNamedWildcard = empty;
            this.key = key;
            if (isNamedWildcard(key)) {
                int len = key.length();
                namedWildcard = key.subSequence(1, len - 1).toString();
            } else {
                namedWildcard = null;
            }
        }

        private boolean isNamedWildcard(String key) {
            return key.charAt(0) == '{'
                && key.charAt(key.length() - 1) == '}';
        }

        public synchronized void insert(String[] path, int index, NodeType value) {
            if (index >= path.length) {
                return;
            }

            final String token = path[index];

            TrieNode<NodeType> node = null;
            if (isNamedWildcard(token)) {
                for (TrieNode<NodeType> child : childrenNamedWildcard) {
                    if (key.equals(token)) {
                        node = child;
                        break;
                    }
                }
                if (node == null) {
                    node = new TrieNode<>(token, null);
                    TrieNode<NodeType>[] tmp = new TrieNode[childrenNamedWildcard.length + 1];
                    System.arraycopy(childrenNamedWildcard, 0, tmp, 0, childrenNamedWildcard.length);
                    tmp[tmp.length - 1] = node;
                    childrenNamedWildcard = tmp;
                }
            } else {
                int keyIndex = insertKey(token);
                if (keyIndex >= 0) {
                    node = children[keyIndex];
                }
                if (node == null) {
                    node = new TrieNode<>(token, null);
                    children[keyIndex] = node;
                }
            }

            if (index == (path.length - 1)) {
                node.value = value;
            } else {
                node.insert(path, index + 1, value);
            }
        }

        private int insertKey(String sequence) {
            int index = indexKey(sequence);
            if (index >= 0) {
                return index;
            } else {
                children = Arrays.copyOf(children, children.length + 1);
                return children.length - 1;
            }
        }

        private int indexKey(String sequence) {
            return indexKey(sequence, 0, sequence.length());
        }

        private int indexKey(String sequence, int start, int end) {
            int size = children.length;
            for (int i = 0; i < size; i++) {
                if (children[i].keyEquals(sequence, start, end)) {
                    return i;
                }
            }
            return -1;
        }

        private boolean keyEquals(String sequence, int start, int end) {
            return end - start == key.length() && key.regionMatches(0, sequence, start, key.length());
        }

        public NodeType retrieve(String path, int start, Map<String, String> params) {
            int len = path.length();
            if (start >= len) {
                return null;
            }
            int end = path.indexOf(separator, start);
            if (end == -1) {
                end = len;
            }
            int keyIndex = indexKey(path, start, end);
            if (keyIndex >= 0) {
                TrieNode<NodeType> node = children[keyIndex];
                final NodeType res;
                if (end == len) {
                    res = node.value;
                } else {
                    res = node.retrieve(path, end + 1, params);
                }
                if (res != null) {
                    return res;
                }
            }

            for (TrieNode<NodeType> child : childrenNamedWildcard) {
                final NodeType res;
                if (end == len) {
                    res = child.value;
                } else {
                    res = child.retrieve(path, end + 1, params);
                }
                if (res != null) {
                    put(params, child, path, start, end);
                    return res;
                }
            }

            return null;
        }

        private void put(Map<String, String> params, TrieNode<NodeType> node, String value, int start, int end) {
            params.put(node.namedWildcard, QueryStringDecoder.decodeComponent(value.substring(start, end)));
        }

        private void prettyPrint(int level, String prefix, boolean last) {
            System.out.print(prefix);
            if (level > 0) {
                if (last) {
                    System.out.print("└── ");
                } else {
                    System.out.print("├── ");
                }
            }
            System.out.println(key + " [" + value + "]");
            TrieNode lastNode = null;
            for (TrieNode<NodeType> node : children) {
                if (node != null) {
                    lastNode = node;
                }
            }
            for (TrieNode<NodeType> node : childrenNamedWildcard) {
                if (node != null) {
                    lastNode = node;
                }
            }
            String childPrefix = prefix;
            if (level > 0) {
                childPrefix = prefix + (last ? "    " : "├── ");
            }
            for (TrieNode<NodeType> child : children) {
                if (child != null) {
                    child.prettyPrint(level + 1, childPrefix, lastNode == child);
                }
            }
            for (TrieNode<NodeType> child : childrenNamedWildcard) {
                child.prettyPrint(level + 1, childPrefix, lastNode == child);
            }
            if (level == 0) {
                System.out.println();
            }
        }
    }
}
