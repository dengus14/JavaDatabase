package com.yourname.db.index;

import java.util.ArrayList;
import java.util.List;

public abstract class BPlusTreeNode {
    protected List<Integer> keys;

    public BPlusTreeNode() {
        this.keys = new ArrayList<>();
    }

    public abstract boolean isLeaf();
}
