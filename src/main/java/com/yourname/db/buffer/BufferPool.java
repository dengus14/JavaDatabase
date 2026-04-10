package com.yourname.db.buffer;

import com.yourname.db.storage.DiskManager;
import com.yourname.db.storage.Page;

import java.util.LinkedHashMap;

public class BufferPool {

    private int capacity;
    private LinkedHashMap<Integer, Frame> map;
    private DiskManager diskManager;

    public BufferPool(int capacity, DiskManager diskManager) {
        this.capacity = capacity;
        this.diskManager = diskManager;
        map = new LinkedHashMap<Integer, Frame>(capacity, 0.75f, true);
    }

    private class Frame {
        private Page page;
        private int pinCount;
        private boolean dirty;



        public Frame(Page page) {
            this.page = page;
            this.pinCount = 1;
            this.dirty = false;
        }
    }
}
