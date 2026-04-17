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

        while (!current.isLeaf()){
            boolean found = false;
            InternalNode newNode = (InternalNode) current;
            for(int i = 0; i < current.keys.size(); i++){
                if(key < current.keys.get(i)){
                    current = newNode.getChildren().get(i);
                    found = true;
                    break;
                }
            }
            if(!found){
                current = newNode.getChildren().getLast();
            }
        }

        if (current.isLeaf()){
            LeafNode newCurr = (LeafNode) current;
            for (int i = 0; i < current.keys.size(); i++){
                if (current.keys.get(i) == key){
                    return newCurr.getRecordIDs().get(i);
                }
            }
        }
        return null;
    }
}
