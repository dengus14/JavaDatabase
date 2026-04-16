package com.yourname.db.index;

public class InternalNode extends BPlusTreeNode{







    @Override
    public boolean isLeaf() {
        return false;
    }
}
