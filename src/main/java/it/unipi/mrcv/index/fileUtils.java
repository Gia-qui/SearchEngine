package it.unipi.mrcv.index;

import it.unipi.mrcv.compression.Unary;
import it.unipi.mrcv.compression.VariableByte;
import it.unipi.mrcv.data_structures.DictionaryElem;
import it.unipi.mrcv.data_structures.Posting;
import it.unipi.mrcv.data_structures.PostingList;
import it.unipi.mrcv.data_structures.SkipElem;
import it.unipi.mrcv.global.Global;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;

import static it.unipi.mrcv.index.SPIMI.decodeTerm;

// a ton of utility functions for reading and writing files
public class fileUtils {

    // functions that delete the files produced by the indexing in order to rerun it
    public static void deleteTempFiles() {
        final File folder = new File("./");
        final File[] files = folder.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(final File dir,
                                  final String name) {
                String regexString = "^(" + Global.prefixDocFiles + "|" + Global.prefixVocFiles + "|" + Global.prefixFreqFiles + ").*";
                return name.matches(regexString);
            }
        });
        for (final File file : files) {
            if (!file.delete()) {
                System.err.println("Can't remove " + file.getAbsolutePath());
            }
        }
    }

    public static void deleteFiles() {
        final File folder = new File("./");
        final File[] files = folder.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(final File dir,
                                  final String name) {
                String regexString = "^(" + Global.finalFreq + "|" + Global.finalDoc + "|" + Global.finalVoc + "|" + Global.skippingFile + "|" + "docIndex" + "|" + "collectionInfo.txt" + ").*";
                return name.matches(regexString);
            }
        });
        for (final File file : files) {
            if (!file.delete()) {
                System.err.println("Can't remove " + file.getAbsolutePath());
            }
        }
    }

    public static void deleteFilesCompressed() {
        final File folder = new File("./");
        final File[] files = folder.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(final File dir,
                                  final String name) {
                String regexString = "^(" + Global.finalFreqCompressed + "|" + Global.finalDocCompressed + "|" + Global.finalVocCompressed + "|" + Global.skippingFile + "|" + "docIndex" + "|" + "collectionInfo.txt" + ").*";
                return name.matches(regexString);
            }
        });
        for (final File file : files) {
            if (!file.delete()) {
                System.err.println("Can't remove " + file.getAbsolutePath());
            }
        }
    }


    // debug function that reads the dictionary and its postinglists from the vocabulary file, return only a
    // block of the postinglist (if the postingList is splitted in blocks)
    public static DictionaryElem binarySearchOnDictionaryBlock(String term, PostingList pl, int nBlock) {
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
                    throw new Exception(term+": word doesn't exists in vocabulary");
                }

            } while ((!readElem.getTerm().equals(term)));
            pl.setTerm(term);
            //We want to return only a portion of the postinglist if skipping is abilited for that term
            if (Global.compression) {
                //return all posting list
                if (readElem.getSkipLen() == 0) {
                    byte[] docsBytes = readCompressed(Global.docIdsChannel, readElem.getOffsetDoc(), readElem.getLengthDocIds());
                    byte[] freqsBytes = readCompressed(Global.frequenciesChannel, readElem.getOffsetFreq(), readElem.getLengthFreq());
                    ArrayList<Integer> docIds = VariableByte.fromByteToArrayInt(docsBytes);
                    ArrayList<Integer> freqs = Unary.unaryToArrayInt(freqsBytes);
                    for (int i = 0; i < docIds.size(); i++) {
                        int docId = docIds.get(i);
                        int freq = freqs.get(i);
                        pl.addPosting(new Posting(docId, freq));
                    }
                } else {
                    SkipElem skipElem = new SkipElem();
                    MappedByteBuffer mbbSkipping = Global.skippingChannel.map(FileChannel.MapMode.READ_ONLY, readElem.getOffsetSkip() + (long) nBlock * SkipElem.size(), SkipElem.size()).load();
                    skipElem.readFromFile(mbbSkipping);
                    byte[] docsBytes = readCompressed(Global.docIdsChannel, skipElem.getOffsetDoc(), skipElem.getDocBlockLen());
                    byte[] freqsBytes = readCompressed(Global.frequenciesChannel, skipElem.getOffsetFreq(), skipElem.getFreqBlockLen());
                    ArrayList<Integer> docIds = VariableByte.fromByteToArrayInt(docsBytes);
                    ArrayList<Integer> freqs = Unary.unaryToArrayInt(freqsBytes);
                    for (int i = 0; i < docIds.size(); i++) {
                        int docId = docIds.get(i);
                        int freq = freqs.get(i);
                        pl.addPosting(new Posting(docId, freq));
                    }
                }

            } else {
                pl.addPostings(readPosting(readElem.getOffsetDoc(), readElem.getLengthDocIds()));
            }

        } catch (Exception e) {
            return null;
        }
        return readElem;
    }

    // debug function to print all the posting blocks of a term
    public static void printAllPostingBlocks(String term) {
        int ns = 0;
        PostingList pl = new PostingList();
        int counter = 0;
        DictionaryElem de = binarySearchOnDictionaryBlock(term, pl, ns);
        if (de.getSkipLen() == 0) {
            pl.printPostingList();
        } else {
            while (counter < de.getCf()) {
                System.out.println("block: " + ns);
                pl.printPostingList();
                pl.getPostings().clear();
                ns++;
                de = binarySearchOnDictionaryBlock(term, pl, ns);
                counter += pl.getPostings().size();

            }

        }

    }


    // debug function that returns the posting list of a term in an uncompressed index
    public static ArrayList<Posting> readPosting(long offset, int length) {
        ArrayList<Posting> ret = new ArrayList<>();
        ArrayList<Integer> docIds = readUncompressed(Global.finalDoc, offset, length);
        ArrayList<Integer> freqs = readUncompressed(Global.finalFreq, offset, length);
        for (int i = 0; i < docIds.size(); i++) {
            ret.add(new Posting(docIds.get(i), freqs.get(i)));
        }
        return ret;
    }

    // function that reads a dictionary element once the location on disk is known
    public static void readEntryDictionary(ByteBuffer reader, FileChannel vocFchan, long offset, DictionaryElem ret) throws IOException {
        vocFchan.read(reader, offset);
        reader.flip();
        int termSize = 40;
        byte[] termBytes = new byte[termSize];
        reader.get(termBytes);
        ret.setTerm(decodeTerm(termBytes));
        ret.setDf(reader.getInt());
        ret.setCf(reader.getInt());
        ret.setOffsetDoc(reader.getLong());
        ret.setOffsetFreq(reader.getLong());
        ret.setLengthDocIds(reader.getInt());
        ret.setLengthFreq(reader.getInt());
        ret.setMaxTF(reader.getInt());
        ret.setOffsetSkip(reader.getLong());
        ret.setSkipLen(reader.getInt());
        ret.setIdf(reader.getDouble());
        ret.setMaxTFIDF(reader.getDouble());
        ret.setMaxBM25(reader.getDouble());

    }

    public static void readDictionaryAndPostingCompressed(String path) {

        readDictionaryAndPostingCompressed(path, null, null);

    }

    public static void readDictionaryAndPostingCompressed(String path, String path2) {
        readDictionaryAndPostingCompressed(path, path2, null);
    }

    // debugging function utilized to read the dictionary and its compressed postinglists
    public static void readDictionaryAndPostingCompressed(String path, String path2, String path3) {
        try (FileChannel vocFchan = (FileChannel) Files.newByteChannel(Paths.get(path),
                StandardOpenOption.READ)) {

            // Size for each term in bytes
            int termSize = 40;
            int entrySize = DictionaryElem.size();
            ByteBuffer buffer = ByteBuffer.allocate(entrySize);

            while (vocFchan.read(buffer) != -1) {
                buffer.flip(); // Prepare the buffer for reading

                if (buffer.remaining() >= entrySize) {
                    // Read term
                    byte[] termBytes = new byte[termSize];
                    buffer.get(termBytes);
                    String term = decodeTerm(termBytes);

                    // Read statistics
                    int df = buffer.getInt();
                    int cf = buffer.getInt();
                    long offsetDoc = buffer.getLong();
                    long offsetFreq = buffer.getLong();
                    int lengthDoc = buffer.getInt();
                    int lengthFreq = buffer.getInt();
                    int maxTF = buffer.getInt();
                    long offsetSkip = buffer.getLong();
                    int skipLen = buffer.getInt();
                    double idf = buffer.getDouble();
                    double maxTFIDF = buffer.getDouble();
                    double maxBM25 = buffer.getDouble();
                    // Print the details
                    System.out.println("Term: " + term);
                    System.out.println("Document Frequency (df): " + df);
                    System.out.println("Collection Frequency (cf): " + cf);
                    System.out.println("Offset Doc: " + offsetDoc);
                    System.out.println("Offset Freq: " + offsetFreq);
                    System.out.println("LengthDoc: " + lengthDoc);
                    System.out.println("LengthFreq: " + lengthFreq);
                    if (path2 != null) {
                        System.out.print("DocIds: ");
                        printCompressedDocIds(path2, lengthDoc, offsetDoc);
                        System.out.println();
                    }
                    if (path3 != null) {
                        System.out.print("Frequencies: ");
                        printCompressedFrequencies(path3, lengthFreq, offsetFreq);
                    }

                    System.out.println("-------------------------");
                } else {
                    // Not enough data for a full dictionary entry, handle partial read or end of file
                    System.err.println("Partial read or end of file reached. Exiting.");
                    break;
                }

                buffer.clear(); // Clear the buffer for the next read
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    // debugging function to print the compressed frequencies of a posting list
    private static void printCompressedFrequencies(String path3, int lengthFreq, long offsetFreq) {
        try {
            RandomAccessFile raf = new RandomAccessFile(new File(path3), "r");
            byte[] b = new byte[lengthFreq];
            raf.seek(offsetFreq);
            raf.read(b);
            ArrayList<Integer> ret = Unary.unaryToArrayInt(b);
            for (int x : ret) {
                System.out.print(x + " ");
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // debugging function to print the compressed docIds of a posting list
    public static void printCompressedDocIds(String path, int length, long offset) {
        try {
            RandomAccessFile raf = new RandomAccessFile(new File(path), "r");
            byte[] b = new byte[length];
            raf.seek(offset);
            raf.read(b);
            ArrayList<Integer> ret = VariableByte.fromByteToArrayInt(
                    b);
            for (int x : ret) {
                System.out.print(x + " ");
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // function that reads a compressed posting list from the disk given location and length
    // return an array of bytes that the call has to decode using the correct de-compression algorithm
    public static byte[] readCompressed(FileChannel fchan, long offset, int lenght) {
        byte[] ret;
        try {
            ByteBuffer buffer = ByteBuffer.allocate(lenght);
            fchan.read(buffer, offset);
            buffer.flip();
            ret = new byte[lenght];
            buffer.get(ret);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return ret;
    }

    // debugging function that reads the compressed docIds and return them in a list
    public static ArrayList<Integer> readCompressedDocIds(FileChannel fchan, long offset, int length) {
        ArrayList<Integer> ret;
        try {
            ByteBuffer buffer = ByteBuffer.allocate(length);
            fchan.read(buffer, offset);
            buffer.flip();
            byte[] b = new byte[length];
            buffer.get(b);
            ret = VariableByte.fromByteToArrayInt(
                    b);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return ret;
    }

    // debugging function that reads the compressed frequencies and return them in a list
    public static ArrayList<Integer> readCompressedFrequencies(FileChannel fchan, long offset, int length) {
        ArrayList<Integer> ret;
        try {
            ByteBuffer buffer = ByteBuffer.allocate(length);
            fchan.read(buffer, offset);
            buffer.flip();
            byte[] b = new byte[length];
            buffer.get(b);
            ret = Unary.unaryToArrayInt(b);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return ret;
    }

    // debugging function that reads the uncompressed docIds or frequences and return them in a list
    public static ArrayList<Integer> readUncompressed(String path, long offset, int length) {
        ArrayList<Integer> ret = new ArrayList<>();
        try {
            RandomAccessFile raf = new RandomAccessFile(new File(path), "r");
            byte[] b = new byte[length * 4];
            raf.seek(offset);
            raf.read(b);
            for (int i = 0; i < length; i++) {
                ret.add(ByteBuffer.wrap(b, i * 4, 4).getInt());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return ret;
    }

    // Function that reads the docNumber of a document from the docIndex file given the docId
    public static String getDocNumber(int docId, RandomAccessFile raf) {
        String docNumber = "";
        try {
            byte[] b = new byte[7];
            raf.seek(docId * 11L);
            raf.read(b);
            docNumber = new String(b);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return docNumber;
    }

}
