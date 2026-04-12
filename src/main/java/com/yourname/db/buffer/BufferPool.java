package com.yourname.db.buffer;

import com.yourname.db.storage.DiskManager;
import com.yourname.db.storage.Page;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public class BufferPool {

    private int capacity;
    private LinkedHashMap<Integer, Frame> map;
    private DiskManager diskManager;

    public BufferPool(int capacity, DiskManager diskManager) {
        this.capacity = capacity;
        this.diskManager = diskManager;
        map = new LinkedHashMap<Integer, Frame>(capacity, 0.75f, true);
    }

    public Page fetchPage(int pageNumber) throws IOException {
        if (map.containsKey(pageNumber)) {
            Frame frame = map.get(pageNumber);
            frame.pinCount++;
            return frame.page;
        }


        else{
            if (map.size() != capacity) {
                Frame frame = new Frame(diskManager.readPage(pageNumber));
                map.put(pageNumber, frame);
                return frame.page;
            }
            else {
                for (Map.Entry<Integer, Frame> entry : map.entrySet()) {
                    if (entry.getValue().pinCount == 0) {
                        if (entry.getValue().dirty) {
                            diskManager.writePage(entry.getValue().page);
                        }
                        map.remove(entry.getKey());
                        Frame frame = new Frame(diskManager.readPage(pageNumber));
                        map.put(pageNumber, frame);
                        return frame.page;
                    }
                }

            }
        }
        throw new IOException("BufferPool is full, all pages pinned");

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
