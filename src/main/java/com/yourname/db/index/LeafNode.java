package com.yourname.db.index;

import com.yourname.db.storage.RecordID;

import java.util.ArrayList;
import java.util.List;

public class LeafNode extends BPlusTreeNode {
    private List<RecordID> recordIDs;

    public List<RecordID> getRecordIDs() {
        return recordIDs;
    }
    public LeafNode next;

    @Override
    public boolean isLeaf() {
        return true;
    }

    public LeafNode(LeafNode next) {
        this.recordIDs = new ArrayList<>();
        this.next = next;
    }

    public void setNext(LeafNode next) {
        this.next = next;
    }

    public LeafNode getNext() {
        return next;
    }
}
