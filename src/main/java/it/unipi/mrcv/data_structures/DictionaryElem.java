package it.unipi.mrcv.data_structures;

import it.unipi.mrcv.compression.Unary;
import it.unipi.mrcv.compression.VariableByte;
import it.unipi.mrcv.global.Global;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

import static it.unipi.mrcv.global.Global.averageDocLength;
import static it.unipi.mrcv.global.Global.collectionLength;
import static it.unipi.mrcv.index.fileUtils.readEntryDictionary;

public class DictionaryElem {
    // term
    private String term;
    // document frequency (n. of documents containing the term)
    private int df;
    // collection frequency (n. of occurrences)
    private int cf;
    // offset of the posting list containing docIds
    private long offsetDoc;
    // offset of the posting list containing frequencies
    private long offsetFreq;

    // length of the posting list
    private int lengthDocIds;

    // length of the posting list (frequencies)
    private int lengthFreq;
    // max term frequency
    private int maxTF;

    // offset of the skipping information
    private long offsetSkip;

    // length of the skipping information
    private int skipLen;

    // inverse document frequency
    private double idf;

    // Term upper bound for TF-IDF
    private double maxTFIDF;

    // Term upper bound for BM25
    private double maxBM25;

    // default constructor
    public DictionaryElem() {
    }

    // constructor
    public DictionaryElem(String term) {
        this.term = term;
        this.df = 1;
        this.cf = 1;
        this.offsetDoc = 0;
        this.offsetFreq = 0;
        this.lengthDocIds = 0;
        this.lengthFreq = 0;
        this.maxTF = 0;
        this.offsetSkip = 0;
        this.skipLen = 0;
        this.idf = 0;
        this.maxTFIDF = 0;
        this.maxBM25 = 0;
    }

    // return the size of the term with only the features used in the SPIMI algorithm
    public static int SPIMIsize() {
        return 60;
    }

    // return the size of the whole dictionary element
    public static int size() {
        return 112;
    }

    // method to read a dictionary element from disk using binary search
    public static DictionaryElem binarySearch(String term) {
        int step = DictionaryElem.size();
        int firstPos = 0;
        int currentPos;
        int previousPos;
        int lastPos;
        ByteBuffer readBuffer = ByteBuffer.allocate(step);
        DictionaryElem readElem = new DictionaryElem();
        FileChannel vocFchan = Global.vocabularyChannel;
        try {
            lastPos = (int) (vocFchan.size() / step);
            currentPos = (firstPos + lastPos) / 2;
            do {
                readBuffer.clear();
                previousPos = currentPos;
                readEntryDictionary(readBuffer, vocFchan, (long) currentPos * step, readElem);
                if (readElem.getTerm().compareTo(term) > 0) {
                    lastPos = currentPos;
                } else {
                    firstPos = currentPos;
                }
                currentPos = (firstPos + lastPos) / 2;
                if (currentPos == previousPos && !readElem.getTerm().equals(term)) {
                    throw new Exception("word doesn't exists in vocabulary");
                }

            } while ((!readElem.getTerm().equals(term)));

        } catch (Exception e) {
            //System.out.println(term + ": " + e.getMessage());
            return null;
        }
        return readElem;
    }

    // compute the statistics of the term
    public void setIdf() {
        this.idf = Math.log10(collectionLength / (double) this.df);
    }

    public void computeMaxTFIDF() {
        this.maxTFIDF = (1 + Math.log10(this.maxTF)) * this.idf;
    }

    // function that computes max BM25 for a given term
    public void computeMaxBM25(int docLength) {
        double k1 = 1.2;
        double b = 0.75;
        double tf = this.maxTF;
        double idf = this.idf;
        double numerator = tf * (k1 + 1);
        double denominator = tf + k1 * (1 - b + b * (docLength / averageDocLength));
        // maxBM25 is the maximum BM25 between the current one and the one computed for the current document
        this.maxBM25 = Math.max(this.maxBM25, idf * numerator / denominator);
    }

    // get and set methods
    public String getTerm() {
        return this.term;
    }

    public void setTerm(String term) {
        this.term = term;
    }

    public int getDf() {
        return this.df;
    }

    public void setDf(int df) {
        this.df = df;
    }

    public int getCf() {
        return this.cf;
    }

    public void setCf(int cf) {
        this.cf = cf;
    }

    public long getOffsetDoc() {
        return this.offsetDoc;
    }

    public void setOffsetDoc(long offsetDoc) {
        this.offsetDoc = offsetDoc;
    }

    public long getOffsetFreq() {
        return this.offsetFreq;
    }

    public void setOffsetFreq(long offsetFreq) {
        this.offsetFreq = offsetFreq;
    }

    public int getLengthDocIds() {
        return this.lengthDocIds;
    }

    public void setLengthDocIds(int lengthDocIds) {
        this.lengthDocIds = lengthDocIds;
    }

    public int getLengthFreq() {
        return this.lengthFreq;
    }

    public void setLengthFreq(int lengthFreq) {
        this.lengthFreq = lengthFreq;
    }

    public int getMaxTF() {
        return this.maxTF;
    }

    public void setMaxTF(int maxTF) {
        this.maxTF = maxTF;
    }

    public long getOffsetSkip() {
        return this.offsetSkip;
    }

    public void setOffsetSkip(long offsetSkip) {
        this.offsetSkip = offsetSkip;
    }

    public int getSkipLen() {
        return this.skipLen;
    }

    public void setSkipLen(int skipLen) {
        this.skipLen = skipLen;
    }

    public double getMaxBM25() {
        return maxBM25;
    }

    public void setMaxBM25(double maxBM25) {
        this.maxBM25 = maxBM25;
    }

    public double getIdf() {
        return idf;
    }

    public void setIdf(double idf) {
        this.idf = idf;
    }

    public double getMaxTFIDF() {
        return maxTFIDF;
    }

    public void setMaxTFIDF(double maxTFIDF) {
        this.maxTFIDF = maxTFIDF;
    }

    // method to write a dictionary element to disk used in the merger
    public void writeElemToDisk(MappedByteBuffer vocBuffer) {
        CharBuffer charBuffer = CharBuffer.allocate(40);
        String term = this.term;
        for (int i = 0; i < term.length() && i < 40; i++)
            charBuffer.put(i, term.charAt(i));

        // Write the term into file
        ByteBuffer truncatedBuffer = ByteBuffer.allocate(40); // Allocate buffer for 40 bytes
        // Encode the CharBuffer into a ByteBuffer
        ByteBuffer encodedBuffer = StandardCharsets.UTF_8.encode(charBuffer);
        // Ensure the buffer is at the start before reading from it
        encodedBuffer.rewind();
        // Transfer bytes to the new buffer
        for (int i = 0; i < 40; i++) {
            truncatedBuffer.put(encodedBuffer.get(i));
        }

        truncatedBuffer.rewind();
        vocBuffer.put(truncatedBuffer);

        // write statistics
        vocBuffer.putInt(df);
        vocBuffer.putInt(cf);
        vocBuffer.putLong(offsetDoc);
        vocBuffer.putLong(offsetFreq);
        vocBuffer.putInt(lengthDocIds);
        vocBuffer.putInt(lengthFreq);
        vocBuffer.putInt(maxTF);
        vocBuffer.putLong(offsetSkip);
        vocBuffer.putInt(skipLen);
        vocBuffer.putDouble(idf);
        vocBuffer.putDouble(maxTFIDF);
        vocBuffer.putDouble(maxBM25);

    }

    // method to write a dictionary element to disk used in the SPIMI algorithm
    public void writeSPIMIElemToDisk(MappedByteBuffer vocBuffer) {
        CharBuffer charBuffer = CharBuffer.allocate(40);
        String term = this.term;
        for (int i = 0; i < term.length() && i < 40; i++)
            charBuffer.put(i, term.charAt(i));

        // Write the term into file
        ByteBuffer truncatedBuffer = ByteBuffer.allocate(40); // Allocate buffer for 40 bytes
        // Encode the CharBuffer into a ByteBuffer
        ByteBuffer encodedBuffer = StandardCharsets.UTF_8.encode(charBuffer);
        // Ensure the buffer is at the start before reading from it
        encodedBuffer.rewind();
        // Transfer bytes to the new buffer
        for (int i = 0; i < 40; i++) {
            truncatedBuffer.put(encodedBuffer.get(i));
        }

        truncatedBuffer.rewind();
        vocBuffer.put(truncatedBuffer);

        // write statistics
        vocBuffer.putInt(df);
        vocBuffer.putInt(cf);
        vocBuffer.putLong(offsetDoc);
        vocBuffer.putInt(lengthDocIds);


    }

    // debug print
    public void printDebug() {
        System.out.println("DEBUG:");
        System.out.println("term: " + term);
        System.out.println("df: " + df);
        System.out.println("docLength: " + lengthDocIds);
        System.out.println("freqLength: " + lengthFreq);
        System.out.println("offsetDoc: " + offsetDoc);
        System.out.println("offsetFreq: " + offsetFreq);
        System.out.println("offsetSkip: " + offsetSkip);
        System.out.println("skipLen: " + skipLen);
        System.out.println("maxTF: " + maxTF);
        System.out.println("idf: " + idf);
        System.out.println("maxTFIDF: " + maxTFIDF);
        System.out.println("maxBM25: " + maxBM25);
    }
}
