package it.unipi.mrcv.index;

import it.unipi.mrcv.data_structures.*;
import it.unipi.mrcv.global.Global;
import it.unipi.mrcv.preprocess.preprocess;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import static it.unipi.mrcv.global.Global.averageDocLength;
import static it.unipi.mrcv.global.Global.collectionLength;


public class SPIMI {
    // counter for the block
    public static int counterBlock = 0;
    public static int numPosting = 0;
    // Dictionary in memory
    public static Dictionary dictionary = new Dictionary();
    // Posting Lists in memory
    public static InvertedIndex postingLists = new InvertedIndex();

    // list that stores the docIndex
    public static List<DocIndexElem> docIndexList = new ArrayList<>();

    // docIndex offset
    public static long offsetDocIndex = 0;


    public static void exeSPIMI(String path) throws IOException, InterruptedException {
        // Max memory usable by the JVM
        long MaxUsableMemory = Runtime.getRuntime().maxMemory() * 80 / 100;
        // docIds start from 0
        int docId = 0;
        // variable that stores the current docNumber
        String documentNumber = null;
        // instantiate the averageDocLength
        averageDocLength = 0;

        try (FileInputStream fileInputStream = new FileInputStream(path);
             GZIPInputStream gzipInputStream = new GZIPInputStream(fileInputStream);
             TarArchiveInputStream tarInputStream = new TarArchiveInputStream(gzipInputStream)) {

            TarArchiveEntry entry;
            while ((entry = tarInputStream.getNextTarEntry()) != null) {
                if (entry.getName().endsWith("collection.tsv")) {
                    BufferedReader reader = new BufferedReader(new InputStreamReader(tarInputStream, StandardCharsets.UTF_8));
                    // read the first line
                    String line = reader.readLine();


                    // read the document collection line by line and execute the SPIMI algorithm
                    while (line != null) {
                        // increase the stored collection length
                        collectionLength++;

                        // split the line in two parts: the first is the document number, the second is the text
                        String[] parts = line.split("\t", 2);
                        try {
                            int number = Integer.parseInt(parts[0]);
                            // document number is saved as string with 7 digits
                            documentNumber = String.format("%07d", number);
                        } catch (NumberFormatException e) {
                            System.err.println("The first part is not an integer. Exiting.");
                            System.exit(1);
                        }

                        // preprocess the line and obtain the tokens
                        List<String> tokens = preprocess.all(parts[1]);

                        // insert docNumber and docLength in the list of DocIndexElems
                        docIndexList.add(new DocIndexElem(documentNumber, tokens.size()));

                        // increase the averageDocLength
                        averageDocLength += tokens.size();

                        // for each term in the line create/update posting lists and dictionary
                        for (String term : tokens) {
                            if (term.length() == 0) {
                                continue;
                            }
                            DictionaryElem entryDictionary = dictionary.getElem(term);
                            // if the term is not in the dictionary we create a new entry
                            if (entryDictionary == null) {
                                dictionary.insertElem(new DictionaryElem(term));
                                postingLists.addPosting(term, new Posting(docId, 1));
                                numPosting++;
                                // if the term is in the dictionary we update the entry
                            } else {
                                entryDictionary.setCf(entryDictionary.getCf() + 1);
                                PostingList list = postingLists.getPostings(term);
                                List<Posting> Postings = list.getPostings();
                                Posting lastPosting = Postings.get(Postings.size() - 1);
                                // if the last posting has the same docId we update the frequency
                                if (lastPosting.getDocid() == docId) {
                                    lastPosting.setFrequency(lastPosting.getFrequency() + 1);
                                    // if the last posting has a different docId we create a new posting and update the document frequency
                                } else {
                                    entryDictionary.setDf(entryDictionary.getDf() + 1);
                                    Postings.add(new Posting(docId, 1));
                                    numPosting++;
                                }
                            }
                        }
                        // increase the docId
                        docId++;
                        // check if the memory is full, in which case the block is written on disk
                        if (Runtime.getRuntime().totalMemory() > MaxUsableMemory) {
                            System.out.printf("(INFO) MAXIMUM PERMITTED USE OF MEMORY ACHIEVED: WRITING BLOCK '%d' ON CURRENT DISC.\n", counterBlock);
                            writeToDisk();
                        }
                        // read the next line if memory is not full
                        line = reader.readLine();
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        // write the last block on disk
        writeToDisk();
        // compute the averageDocLength
        averageDocLength = averageDocLength / collectionLength;
    }

    // function that writes the block on disk
    private static void writeToDisk() throws IOException, InterruptedException {
        // instantiate the fileUtils class
        try (FileChannel docIndexFchan = FileChannel.open(Paths.get(Global.prefixDocIndex), StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.CREATE); FileChannel docsFchan = (FileChannel) Files.newByteChannel(Paths.get(Global.prefixDocFiles + counterBlock), StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.CREATE); FileChannel freqsFchan = (FileChannel) Files.newByteChannel(Paths.get(Global.prefixFreqFiles + counterBlock), StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.CREATE); FileChannel vocabularyFchan = (FileChannel) Files.newByteChannel(Paths.get(Global.prefixVocFiles + counterBlock), StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.CREATE)) {

            // instantiation of MappedByteBuffer for integer list of docIndex
            MappedByteBuffer docIndexBuffer = docIndexFchan.map(FileChannel.MapMode.READ_WRITE, offsetDocIndex, docIndexList.size() * 11L);
            // instantiation of MappedByteBuffer for integer list of docids
            MappedByteBuffer docsBuffer = docsFchan.map(FileChannel.MapMode.READ_WRITE, 0, numPosting * 4L);
            // instantiation of MappedByteBuffer for integer list of freqs
            MappedByteBuffer freqsBuffer = freqsFchan.map(FileChannel.MapMode.READ_WRITE, 0, numPosting * 4L);
            // instantiation of MappedByteBuffer for vocabulary
            MappedByteBuffer vocBuffer = vocabularyFchan.map(FileChannel.MapMode.READ_WRITE, 0, dictionary.SPIMIsize());

            // write the docIndexList on disk by appending on the docIndex file
            for (int i = 0; i < docIndexList.size(); i++) {
                docIndexBuffer.put(docIndexList.get(i).getDocumentNumber().getBytes());
                docIndexBuffer.putInt(docIndexList.get(i).getDocLength());
            }
            // update the offset of the docIndex
            offsetDocIndex += docIndexList.size() * 11L;

            // for each term in the term-postingList treemap write everything to file
            for (Map.Entry<String, PostingList> entry : postingLists.getTree().entrySet()) {
                // update the offset of the term in the dictionary
                DictionaryElem dictionaryElem = dictionary.getElem(entry.getKey());
                dictionaryElem.setOffsetDoc(docsBuffer.position());
                int counter = 0;
                // write the postings in the respective docIds and frequencies files
                for (Posting posting : entry.getValue().getPostings()) {
                    docsBuffer.putInt(posting.getDocid());
                    freqsBuffer.putInt(posting.getFrequency());
                    counter++;
                }

                // update the length of the posting list in the dictionary
                dictionaryElem.setLengthDocIds(counter);
                dictionaryElem.setLengthFreq(counter);
                dictionaryElem.writeSPIMIElemToDisk(vocBuffer);


            }
        }

        // increase the counter of the block
        counterBlock++;
        // clear the memory
        postingLists.clear();
        dictionary.clear();
        docIndexList.clear();
        // hope that the garbage collector will free the memory
        System.gc();
        Thread.sleep(1500);
        // reset data structures
        dictionary = new Dictionary();
        postingLists = new InvertedIndex();
        // reset the number of postings
        numPosting = 0;
    }


    // function that reads the docIds file OR the freqs file and prints them
    public static void readIndex(String path) {
        try (FileInputStream fis = new FileInputStream(path); FileChannel fileChannel = fis.getChannel()) {

            ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);

            while (fileChannel.read(buffer) != -1) {
                buffer.flip(); // Prepare the buffer for reading

                if (buffer.remaining() >= Integer.BYTES) {
                    int frequency = buffer.getInt();
                    System.out.println(frequency); // Print the integer
                } else {
                    // Not enough bytes for a full integer, handle partial read or end of file
                    System.err.println("Partial read or end of file reached. Exiting.");
                    break;
                }

                buffer.clear(); // Clear the buffer for the next read
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static void readDictionary(String path) {
        try (FileChannel vocFchan = (FileChannel) Files.newByteChannel(Paths.get(path), StandardOpenOption.READ)) {

            // Size for each term in bytes
            int termSize = 40;
            // Sizes for the other integers and longs
            int intSize = Integer.BYTES; // 4 bytes
            int longSize = Long.BYTES;   // 8 bytes
            // Total SPIMIsize of one dictionary entry
            int entrySize = termSize + 4 * intSize + 2 * longSize;

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

                    // Print the details
                    System.out.println("Term: " + term);
                    System.out.println("Document Frequency (df): " + df);
                    System.out.println("Collection Frequency (cf): " + cf);
                    System.out.println("Offset Doc: " + offsetDoc);
                    System.out.println("Offset Freq: " + offsetFreq);
                    System.out.println("Length: " + lengthDoc);
                    System.out.println("LengthFreq: " + lengthFreq);
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


    // function that decodes the term from the byte array
    public static String decodeTerm(byte[] termBytes) {
        // Create a ByteBuffer from the byte array
        ByteBuffer termBuffer = ByteBuffer.wrap(termBytes);
        // Decode the ByteBuffer into a CharBuffer
        CharBuffer charBuffer = StandardCharsets.UTF_8.decode(termBuffer);
        // Convert CharBuffer to String
        return charBuffer.toString().trim(); // Trim the string in case there are any zero padding bytes
    }


    public static void readDictionaryToFile(String inputPath, String outputPath) {
        try (FileChannel vocFchan = FileChannel.open(Paths.get(inputPath), StandardOpenOption.READ); BufferedWriter writer = new BufferedWriter(new FileWriter(outputPath))) {
            int termSize = 40;
            int entrySize = DictionaryElem.size();

            ByteBuffer buffer = ByteBuffer.allocate(entrySize);

            while (vocFchan.read(buffer) != -1) {
                buffer.flip();

                if (buffer.remaining() >= entrySize) {
                    byte[] termBytes = new byte[termSize];
                    buffer.get(termBytes);
                    String term = decodeTerm(termBytes);

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

                    writer.write("Term: " + term + "\n");
                    writer.write("Document Frequency (df): " + df + "\n");
                    writer.write("Collection Frequency (cf): " + cf + "\n");
                    writer.write("Offset Doc: " + offsetDoc + "\n");
                    writer.write("Offset Freq: " + offsetFreq + "\n");
                    writer.write("Length: " + lengthDoc + "\n");
                    writer.write("LengthFreq: " + lengthFreq + "\n");
                    writer.write("MaxTF: " + maxTF + "\n");
                    writer.write("OffsetSkip:" + offsetSkip + "\n");
                    writer.write("skipLen:" + skipLen + "\n");
                    writer.write("-------------------------\n");
                } else {
                    writer.write("Partial read or end of file reached. Exiting.\n");
                    break;
                }

                buffer.clear();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}

