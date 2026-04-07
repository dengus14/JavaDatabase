package com.yourname.db.storage;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

public class DiskManager {

    private RandomAccessFile file;
    private String fileName;
    private int numPages;




    public DiskManager(String fileName) throws IOException {
        this.fileName = fileName;
        file = new RandomAccessFile(fileName, "rw");
        this.numPages = (int) (file.length() / Page.PAGE_SIZE);
    }

    //this method writes Page's information from memory to disk
    public void writePage(Page page) throws IOException{
        int positionToWrite = page.getPageNumber() * Page.PAGE_SIZE;
        file.seek(positionToWrite);
        byte[] data = page.getData();
        file.write(data);
    }

    public Page readPage(int pageNumber) throws IOException{
        int positionToRead = pageNumber * Page.PAGE_SIZE;
        file.seek(positionToRead);
        byte[] array = new byte[Page.PAGE_SIZE];
        file.read(array);

        return new Page(pageNumber,ByteBuffer.wrap(array));
    }
}
