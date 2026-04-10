package com.yourname.db.buffer;

import com.yourname.db.storage.Page;

public class BufferPool {

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
