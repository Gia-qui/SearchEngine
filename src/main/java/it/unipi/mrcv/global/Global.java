package it.unipi.mrcv.global;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class Global {
    // file names
    public static final String prefixDocFiles = "doc_";
    public static final String prefixVocFiles = "voc_";
    public static final String prefixFreqFiles = "freq_";
    public static final String prefixDocIndex = "docIndex";
    public static final String finalVoc = "vocabulary";
    public static final String finalDoc = "docIds";
    public static final String finalFreq = "frequencies";
    public static final String skippingFile = "skipping";
    public static final String finalVocCompressed = "vocabularyCompressed";
    public static final String finalDocCompressed = "docIdsCompressed";
    public static final String finalFreqCompressed = "frequenciesCompressed";
    public static final String finalStopWordsFile = "stopwords-en.txt";
    public static final String collectionInfoFile = "collectionInfo.txt";

    // flags
    public static boolean compression = true;
    public static boolean stem = true;
    public static boolean stopWords = true;
    public static boolean isBM25 = true;
    public static boolean indexing = false;

    // global variables
    public static int collectionLength;
    public static double averageDocLength;
    public static HashSet<String> stopWordsSet;
    public static ArrayList<Integer> docLengths = new ArrayList<>();
    public static FileChannel docIndexChannel;
    public static FileChannel vocabularyChannel;
    public static FileChannel docIdsChannel;
    public static FileChannel frequenciesChannel;
    public static FileChannel skippingChannel;

    // method to load the global variables and the initial configurations based on the launch parameters
    public static void load() {


        if (!indexing) {
            // load collection length and average doc length
            try {
                List<String> collectionInfo = Files.readAllLines(Paths.get("collectionInfo.txt"));
                collectionLength = Integer.parseInt(collectionInfo.get(0));
                averageDocLength = Double.parseDouble(collectionInfo.get(1));
                compression = Boolean.parseBoolean(collectionInfo.get(2));
                stopWords = Boolean.parseBoolean(collectionInfo.get(3));
                stem = Boolean.parseBoolean(collectionInfo.get(4));
            } catch (IOException e) {
                System.out.println("Can't read collectionInfo file");
            }
            //initialize the filechannels in order to not open more times than necessary
            try {
                docIndexChannel = FileChannel.open(Paths.get(prefixDocIndex), StandardOpenOption.READ);
                if (compression) {
                    vocabularyChannel = FileChannel.open(Paths.get(finalVocCompressed), StandardOpenOption.READ);
                    docIdsChannel = FileChannel.open(Paths.get(finalDocCompressed), StandardOpenOption.READ);
                    frequenciesChannel = FileChannel.open(Paths.get(finalFreqCompressed), StandardOpenOption.READ);
                } else {
                    vocabularyChannel = FileChannel.open(Paths.get(finalVoc), StandardOpenOption.READ);
                    docIdsChannel = FileChannel.open(Paths.get(finalDoc), StandardOpenOption.READ);
                    frequenciesChannel = FileChannel.open(Paths.get(finalFreq), StandardOpenOption.READ);
                }
                skippingChannel = FileChannel.open(Paths.get(skippingFile), StandardOpenOption.READ);
            } catch (IOException e) {
                System.out.println("Can't open channel");
                e.printStackTrace();
            }
            int chunkSize = (collectionLength * 11) / 4;
            ByteBuffer buffer = ByteBuffer.allocate(chunkSize);
            // load from the docIndex file the docLengths and store them in a list
            try (FileChannel channel = docIndexChannel) {
                int bytesRead;
                while ((bytesRead = channel.read(buffer)) != -1) {
                    buffer.flip(); // Switch the buffer from writing mode to reading mode

                    while (buffer.remaining() >= 11) {
                        buffer.position(buffer.position() + 7); // Skip the 7-byte string
                        int value = buffer.getInt(); // Read the integer
                        docLengths.add(value);
                    }

                    buffer.compact(); // Prepare the buffer for writing again
                }
            } catch (IOException e) {
                System.out.println("Can't read docIndex file");
            }
        }
        try {
            if (stopWords) {
                List<String> stopWordsList = Files.readAllLines(Paths.get(finalStopWordsFile));
                stopWordsSet = new HashSet<>(stopWordsList);
            }
        } catch (IOException e) {
            System.out.println("Can't read Stopword file, flag stopwords set to false");
            stopWords = false;
        }


    }
}
