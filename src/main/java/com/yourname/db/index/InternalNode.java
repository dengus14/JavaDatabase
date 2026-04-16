package com.yourname.db.index;

import java.util.ArrayList;
import java.util.List;

public class InternalNode extends BPlusTreeNode{


    private List<BPlusTreeNode> children;

    public InternalNode() {
        super();
        this.children = new ArrayList<>();
    }

    @Override
    public boolean isLeaf() {
        return false;
    }
}
