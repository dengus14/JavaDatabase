package com.yourname.db.index;

import com.yourname.db.storage.RecordID;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

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


    public void insert(int key, RecordID rid){
        Deque<InternalNode> stack = new ArrayDeque<>();
        BPlusTreeNode current = root;
        boolean needsNewRoot = false;

        while (!current.isLeaf()){
            InternalNode newNode = (InternalNode) current;
            stack.push(newNode);
            boolean found = false;
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

        LeafNode newCurr = (LeafNode) current;
        boolean inserted = false;
        for (int i = 0; i < newCurr.keys.size(); i++){
            if (key < newCurr.keys.get(i)){
                newCurr.keys.add(i, key);
                newCurr.getRecordIDs().add(i, rid);
                inserted = true;
                break;
            }
        }
        if (!inserted){
            newCurr.keys.add(key);
            newCurr.getRecordIDs().add(rid);
        }

        if(newCurr.keys.size() == order){
            BPlusTreeNode newCurr2 = new LeafNode(newCurr.getNext());
            List<Integer> rightKeys = new ArrayList<>(newCurr.keys.subList(order / 2, newCurr.keys.size()));
            List<RecordID> rightRecordIDs = new ArrayList<>(newCurr.getRecordIDs().subList(order / 2, newCurr.getRecordIDs().size()));
            newCurr.keys.subList(order / 2, newCurr.keys.size()).clear();
            newCurr.getRecordIDs().subList(order / 2, newCurr.getRecordIDs().size()).clear();
            ((LeafNode) newCurr2).getRecordIDs().addAll(rightRecordIDs);
            newCurr2.keys.addAll(rightKeys);
            newCurr.setNext((LeafNode) newCurr2);
            int middleKey = rightKeys.get(0);

            boolean stackWasEmpty = stack.isEmpty();
            while(!stack.isEmpty()){
                InternalNode popped = stack.pop();

                inserted = false;
                for (int i = 0; i < popped.keys.size(); i++){
                    if (middleKey < popped.keys.get(i)){
                        popped.keys.add(i, middleKey);
                        popped.getChildren().add(i + 1, newCurr2);
                        if (popped.keys.size() == order){
                            int midIndex = order / 2;
                            middleKey = popped.keys.get(midIndex);
                            rightKeys = new ArrayList<>(popped.keys.subList(midIndex + 1, popped.keys.size()));
                            List<BPlusTreeNode> children = new ArrayList<>(popped.getChildren().subList(midIndex + 1, popped.getChildren().size()));
                            popped.getChildren().subList(midIndex + 1, popped.getChildren().size()).clear();
                            popped.keys.subList(midIndex, popped.keys.size()).clear();
                            InternalNode newInternal = new InternalNode();
                            newInternal.keys.addAll(rightKeys);
                            newInternal.getChildren().addAll(children);
                            newCurr2 = newInternal;
                            needsNewRoot = true;
                        } else {
                            needsNewRoot = false;
                            inserted = true;
                            break;
                        }

                        break;
                    }
                }
                if (!needsNewRoot && inserted) break;
                if (!inserted){
                    popped.keys.add(middleKey);
                    popped.getChildren().add(newCurr2);
                    if (popped.keys.size() == order){
                        int midIndex = order / 2;
                        middleKey = popped.keys.get(midIndex);
                        rightKeys = new ArrayList<>(popped.keys.subList(midIndex + 1, popped.keys.size()));
                        List<BPlusTreeNode> children = new ArrayList<>(popped.getChildren().subList(midIndex + 1, popped.getChildren().size()));
                        popped.getChildren().subList(midIndex + 1, popped.getChildren().size()).clear();
                        popped.keys.subList(midIndex, popped.keys.size()).clear();
                        InternalNode newInternal = new InternalNode();
                        newInternal.keys.addAll(rightKeys);
                        newInternal.getChildren().addAll(children);
                        newCurr2 = newInternal;
                        needsNewRoot = true;
                    } else {
                        break;
                    }
                }
            }

            if (needsNewRoot || stackWasEmpty) {
                InternalNode newRoot = new InternalNode();
                newRoot.keys.add(middleKey);
                newRoot.getChildren().add(root);
                newRoot.getChildren().add(newCurr2);
                root = newRoot;
            }
        }
    }


}
