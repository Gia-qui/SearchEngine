package it.unipi.mrcv.data_structures;

import it.unipi.mrcv.global.Global;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;

public class SkipElem {
    // Last docID in the block
    private int docID;

    // offset of the block relative to the docIds
    private long offsetDoc;

    // docId block length
    private int docBlockLen;

    // offset of the block relative to the frequencies
    private long offsetFreq;

    // frequency block length
    private int freqBlockLen;

    // default constructor
    public SkipElem() {
        this.docID = 0;
        this.offsetDoc = 0;
        this.docBlockLen = 0;
        this.offsetFreq = 0;
        this.freqBlockLen = 0;
    }

    // return the size of the skipping element
    public static int size() {
        return Integer.BYTES + Long.BYTES + Integer.BYTES + Long.BYTES + Integer.BYTES;
    }

    // method that reads the skip list from the file for a posting list and returns it
    public static ArrayList<SkipElem> readSkipList(long offset, int n) throws IOException {
        ArrayList<SkipElem> skipElems = new ArrayList<>(n);
        MappedByteBuffer mbbSkipping = Global.skippingChannel.map(FileChannel.MapMode.READ_ONLY, offset, (long) SkipElem.size() * n).load();
        for (int i = 0; i < n; i++) {
            SkipElem skipElem = new SkipElem();
            skipElem.readFromFile(mbbSkipping);
            skipElems.add(skipElem);
        }
        return skipElems;
    }

    // getters and setters
    public int getDocID() {
        return docID;
    }

    public void setDocID(int docID) {
        this.docID = docID;
    }

    public long getOffsetDoc() {
        return offsetDoc;
    }

    public void setOffsetDoc(long offsetDoc) {
        this.offsetDoc = offsetDoc;
    }

    public int getDocBlockLen() {
        return docBlockLen;
    }

    public void setDocBlockLen(int docBlockLen) {
        this.docBlockLen = docBlockLen;
    }

    public long getOffsetFreq() {
        return offsetFreq;
    }

    public void setOffsetFreq(long offsetFreq) {
        this.offsetFreq = offsetFreq;
    }

    public int getFreqBlockLen() {
        return freqBlockLen;
    }

    public void setFreqBlockLen(int freqBlockLen) {
        this.freqBlockLen = freqBlockLen;
    }

    // write the skip element to the file using the mapped byte buffer
    public void writeToFile(MappedByteBuffer buffer) {
        buffer.putInt(docID);
        buffer.putLong(offsetDoc);
        buffer.putInt(docBlockLen);
        buffer.putLong(offsetFreq);
        buffer.putInt(freqBlockLen);
    }

    // read the skip element from the file using the mapped byte buffer
    public void readFromFile(MappedByteBuffer buffer) {
        docID = buffer.getInt();
        offsetDoc = buffer.getLong();
        docBlockLen = buffer.getInt();
        offsetFreq = buffer.getLong();
        freqBlockLen = buffer.getInt();
    }

    // debug method to print the skip element
    public void printDebug() {
        System.out.printf("DocID: %d - OffsetDoc: %d - DocBlockLen: %d - OffsetFreq: %d - FreqBlockLen: %d\n",
                docID, offsetDoc, docBlockLen, offsetFreq, freqBlockLen);
    }
}
