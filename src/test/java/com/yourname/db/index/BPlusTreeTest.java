package com.yourname.db.index;

import com.yourname.db.storage.RecordID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class BPlusTreeTest {

    private BPlusTree tree;

    @BeforeEach
    void setUp() {
        tree = new BPlusTree(3);
    }

    @Test
    void searchOnEmptyTreeReturnsNull() {
        assertNull(tree.search(42));
    }

    @Test
    void insertOneKeyAndSearchFindsIt() {
        RecordID rid = new RecordID(1, 0);
        tree.insert(10, rid);

        RecordID found = tree.search(10);
        assertNotNull(found);
        assertEquals(rid, found);
    }

    @Test
    void insertMultipleKeysWithoutSplitAllSearchable() {
        // Order 3 means a leaf splits at 3 keys, so 2 keys fit without a split
        RecordID rid1 = new RecordID(1, 0);
        RecordID rid2 = new RecordID(2, 1);
        tree.insert(10, rid1);
        tree.insert(20, rid2);

        assertEquals(rid1, tree.search(10));
        assertEquals(rid2, tree.search(20));
    }

    @Test
    void insertEnoughKeysToTriggerLeafSplitAllSearchable() {
        // 3 keys in order-3 tree triggers a leaf split
        RecordID rid1 = new RecordID(1, 0);
        RecordID rid2 = new RecordID(2, 1);
        RecordID rid3 = new RecordID(3, 2);
        tree.insert(10, rid1);
        tree.insert(20, rid2);
        tree.insert(30, rid3);

        assertEquals(rid1, tree.search(10));
        assertEquals(rid2, tree.search(20));
        assertEquals(rid3, tree.search(30));
    }

    @Test
    void insertEnoughKeysToTriggerRootSplitAllSearchable() {
        // Order 3: internal nodes also split at 3 keys.
        // Inserting 7 keys should cause multiple splits including a root split.
        RecordID[] rids = new RecordID[7];
        for (int i = 0; i < 7; i++) {
            rids[i] = new RecordID(i, i);
            tree.insert((i + 1) * 10, rids[i]);
        }

        for (int i = 0; i < 7; i++) {
            RecordID found = tree.search((i + 1) * 10);
            assertNotNull(found, "Key " + ((i + 1) * 10) + " should be found");
            assertEquals(rids[i], found);
        }
    }

    @Test
    void searchForNonexistentKeyReturnsNull() {
        tree.insert(10, new RecordID(1, 0));
        tree.insert(20, new RecordID(2, 1));

        assertNull(tree.search(99));
    }

    // Known bug: BPlusTree.insert loses keys during splits with out-of-order insertion.
    // This test correctly exposes the issue — search(50) returns null after splits.
    @Test
    void keysInsertedOutOfOrderAreAllSearchable() {
        int[] keys = {50, 10, 40, 20, 30};
        RecordID[] rids = new RecordID[keys.length];
        for (int i = 0; i < keys.length; i++) {
            rids[i] = new RecordID(i, i);
            tree.insert(keys[i], rids[i]);
        }

        for (int i = 0; i < keys.length; i++) {
            RecordID found = tree.search(keys[i]);
            assertNotNull(found, "Key " + keys[i] + " should be found");
            assertEquals(rids[i], found);
        }
    }

    @Test
    void afterSplitsLinkedListOrderIsCorrect() throws Exception {
        // Insert enough keys to cause splits, then walk the leaf linked list
        // and verify keys are in ascending order across leaves.
        int[] keys = {30, 10, 50, 20, 40, 60, 70};
        for (int i = 0; i < keys.length; i++) {
            tree.insert(keys[i], new RecordID(i, i));
        }

        // Access the root via reflection to walk down to the leftmost leaf
        Field rootField = BPlusTree.class.getDeclaredField("root");
        rootField.setAccessible(true);
        BPlusTreeNode node = (BPlusTreeNode) rootField.get(tree);
        while (!node.isLeaf()) {
            node = ((InternalNode) node).getChildren().get(0);
        }

        // Walk the linked list and collect all keys in order
        LeafNode leaf = (LeafNode) node;
        int previousKey = Integer.MIN_VALUE;
        int totalKeys = 0;
        while (leaf != null) {
            List<Integer> leafKeys = leaf.keys;
            // Keys within a leaf should be sorted
            for (int k : leafKeys) {
                assertTrue(k > previousKey,
                        "Key " + k + " should be greater than previous key " + previousKey);
                previousKey = k;
                totalKeys++;
            }
            leaf = leaf.getNext();
        }

        assertEquals(keys.length, totalKeys, "All keys should be present across leaves");
    }

    @Test
    void searchPerformanceWith1000Keys() {
        int count = 1000;
        for (int i = 0; i < count; i++) {
            tree.insert(i, new RecordID(i, i));
        }

        int target = 777;

        // Warm up JIT
        for (int i = 0; i < 1000; i++) {
            tree.search(target);
        }

        // Timed run — average over 10,000 searches
        int iterations = 10_000;
        long start = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            tree.search(target);
        }
        long elapsed = System.nanoTime() - start;

        double avgNanos = (double) elapsed / iterations;
        System.out.println("=== B+ Tree Search Performance (order=3, 1000 keys) ===");
        System.out.println("  Total for " + iterations + " searches: " + (elapsed / 1_000_000.0) + " ms");
        System.out.println("  Average per search: " + avgNanos + " ns (" + (avgNanos / 1_000_000.0) + " ms)");

        // Sanity check — the search still returns the right result
        RecordID found = tree.search(target);
        assertNotNull(found);
        assertEquals(new RecordID(target, target), found);
    }
}
