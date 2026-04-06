package com.yourname.db.storage;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class PageTest {

    @Test
    void testHasSpaceOnEmptyPage() {
        Page page = new Page(0);
        assertTrue(page.hasSpace(100));
    }

    @Test
    void testHasSpaceReturnsFalseWhenFull() {
        Page page = new Page(0);
        // a row so large it can't possibly fit
        assertFalse(page.hasSpace(Page.PAGE_SIZE));
    }
}
