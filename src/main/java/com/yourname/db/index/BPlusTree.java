package com.yourname.db.index;

import com.yourname.db.storage.RecordID;

public class BPlusTree {

    private BPlusTreeNode root;
    private int order;

    public BPlusTree(int order) {
        this.order = order;
        root = new LeafNode(null);
    }

    public RecordID search(int key){
        BPlusTreeNode current = root;

        if (current.isLeaf()){
            LeafNode newCurr = (LeafNode) current;
            for (int i = 0; i < current.keys.size(); i++){
                if (i == key){
                    return newCurr.getRecordIDs().get(i);
                }
            }
        }

        //continue implementing here



        return null;
    }
}
