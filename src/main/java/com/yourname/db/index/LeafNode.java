package com.yourname.db.index;

import com.yourname.db.storage.RecordID;

import java.util.ArrayList;
import java.util.List;

public class LeafNode extends BPlusTreeNode {
    private List<RecordID> recordIDs;
    private LeafNode next;

    @Override
    public boolean isLeaf() {
        return true;
    }

    public LeafNode(LeafNode next) {
        this.recordIDs = new ArrayList<>();
        this.next = next;
    }
}
