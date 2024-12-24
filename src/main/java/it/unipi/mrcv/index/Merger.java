package it.unipi.mrcv.index;

import it.unipi.mrcv.compression.Unary;
import it.unipi.mrcv.compression.VariableByte;
import it.unipi.mrcv.data_structures.Comparators.ComparatorTerm;
import it.unipi.mrcv.data_structures.DictionaryElem;
import it.unipi.mrcv.data_structures.SkipElem;
import it.unipi.mrcv.data_structures.termBlock;
import it.unipi.mrcv.global.Global;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

import static it.unipi.mrcv.global.Global.collectionLength;
import static it.unipi.mrcv.global.Global.skippingFile;

//java class that merges the partial indexes composed of the indexes with the docids and the indexes with the frequencies
public class Merger {

    //java class that merges the partial indexes composed of the indexes with the docids and the indexes with the frequencies

    public static int num_blocks = SPIMI.counterBlock;

    //flag compression
    public static boolean compression = Global.compression;


    public static void Merge() throws IOException {
        // temporaryElem contains the final vocabulary entry that is being build
        termBlock temporaryElem = new termBlock();
        // currentBlock contains the current vocabulary entry that is being read from a partial vocabulary
        termBlock currentBlock;
        // termBytes is a buffer used to read the term from the vocabulary
        byte[] termBytes = new byte[40];
        // docOff and freqOff are used to store the increasing offset at each new docId or frequency
        long docOff = 0;
        long freqOff = 0;
        // latestDocOff and latestFreqOff are used to store the offset of the latest vocabulary entry
        long latestDocOff;
        long latestFreqOff;
        // offset of the skip information
        long skipOff = 0;
        // termNumber is used to store the number of the vocabulary entry, used to write the vocabulary at the correct position
        long termNumber = 0;
        // temporaryDocIds and temporaryFreqs are used to store the docIds and frequencies of the current vocabulary entry
        List<Integer> temporaryDocIds = new ArrayList<>();
        List<Integer> temporaryFreqs = new ArrayList<>();
        // pQueueElems is used to store the first vocabulary entry of each partial vocabulary, all added to the priority queue
        List<termBlock> pQueueElems = new ArrayList<>();
        // docsBuffer, freqsBuffer, vocBuffer and skipBuffer are used to write the docIds,
        // frequencies, vocabulary entries and skipping information in the final files
        MappedByteBuffer docsBuffer;
        MappedByteBuffer freqsBuffer;
        MappedByteBuffer vocBuffer;
        MappedByteBuffer skipBuffer;
        // docPointers, freqPointers and vocPointers are used to store the pointers to the partial vocabularies
        List<RandomAccessFile> docPointers = new ArrayList<>();
        List<RandomAccessFile> freqPointers = new ArrayList<>();
        List<RandomAccessFile> vocPointers = new ArrayList<>();
        // docIndex is the file containing the document lengths
        RandomAccessFile docIndex = new RandomAccessFile(Global.prefixDocIndex, "r");
        // docLengths is the array containing the document lengths
        int[] docLengths = new int[collectionLength];
        // pQueue is the priority queue used to store the vocabulary entries
        PriorityQueue<termBlock> pQueue = new PriorityQueue<termBlock>(num_blocks, new ComparatorTerm());
        // blockLength is the length of the blocks used for skipping
        int blockLength;
        // ArrayList used to store the skip elements of the current term
        ArrayList<SkipElem> skipList = new ArrayList<>();
        // if flag compression is set on false change files names
        String pathDocs = !Global.compression ? Global.finalDoc : Global.finalDocCompressed;
        String pathFreqs = !Global.compression ? Global.finalFreq : Global.finalFreqCompressed;
        String pathVoc = !Global.compression ? Global.finalVoc : Global.finalVocCompressed;

        // initialize the pointers to the partial vocabularies and the priority queue
        for (int i = 0; i < num_blocks; i++) {
            try {
                RandomAccessFile p = new RandomAccessFile(Global.prefixVocFiles + i, "r");
                p.seek(0); //set the pointer to 0
                vocPointers.add(p);
                RandomAccessFile d = new RandomAccessFile(Global.prefixDocFiles + i, "r");
                d.seek(0); //set the pointer to 0
                docPointers.add(d);
                RandomAccessFile q = new RandomAccessFile(Global.prefixFreqFiles + i, "r");
                q.seek(0); //set the pointer to 0
                freqPointers.add(q);

                pQueueElems.add(new termBlock());

                readSPIMIEntryFromDictionary(p, i, pQueueElems.get(i), termBytes);
                pQueue.add(pQueueElems.get(i));

            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        // populate the docLengths array with the document lengths
        for (int i = 0; i < collectionLength; i++) {
            docIndex.seek(i * 11L + 7);
            docLengths[i] = docIndex.readInt();
        }
        // close the docIndex file
        docIndex.close();


        // initialize the final files
        try (

                FileChannel docsFchan = (FileChannel) Files.newByteChannel(Paths.get(pathDocs),
                        StandardOpenOption.WRITE,
                        StandardOpenOption.READ,
                        StandardOpenOption.CREATE);
                FileChannel freqsFchan = (FileChannel) Files.newByteChannel(Paths.get(pathFreqs),
                        StandardOpenOption.WRITE,
                        StandardOpenOption.READ,
                        StandardOpenOption.CREATE);
                FileChannel vocabularyFchan = (FileChannel) Files.newByteChannel(Paths.get(pathVoc),
                        StandardOpenOption.WRITE,
                        StandardOpenOption.READ,
                        StandardOpenOption.CREATE);
                FileChannel skipFchan = (FileChannel) Files.newByteChannel(Paths.get(skippingFile),
                        StandardOpenOption.WRITE,
                        StandardOpenOption.READ,
                        StandardOpenOption.CREATE)) {


            // the whole merge is done in this while loop
            while (!pQueue.isEmpty()) {
                // currentBlock is the vocabulary entry that is being read from the priority queue
                currentBlock = pQueue.poll();
                temporaryElem.copyBlock(currentBlock);
                String term = currentBlock.getTerm();
                // the offsets of the docIds and frequencies are updated to the starting offset of the current term
                latestFreqOff = freqOff;
                latestDocOff = docOff;
                // the docIds and frequencies of the current term are read from the block where the term was found and added to the temporary lists
                readLineFromDocId(docPointers.get(temporaryElem.getNumBlock()), temporaryElem.getDictionaryElem().getLengthDocIds(), temporaryDocIds);
                readLineFromFreq(freqPointers.get(temporaryElem.getNumBlock()), temporaryElem.getDictionaryElem().getLengthFreq(), temporaryFreqs);
                // if the block is not finished, the next vocabulary entry is read and added to the priority queue
                if (!isEndOfFile(vocPointers.get(currentBlock.getNumBlock()))) {
                    readSPIMIEntryFromDictionary(vocPointers.get(currentBlock.getNumBlock()), currentBlock.getNumBlock(), pQueueElems.get(currentBlock.getNumBlock()), termBytes);
                    pQueue.add(pQueueElems.get(currentBlock.getNumBlock()));
                }

                // the priority queue is checked to see if there are other vocabulary entries with the same term
                while (!pQueue.isEmpty() && term.equals(pQueue.peek().getTerm())) {
                    // the read from the queue every element with the same term and update the statistics of the temporaryElem
                    currentBlock = pQueue.poll();
                    int blockNumber = currentBlock.getNumBlock();
                    temporaryElem.getDictionaryElem().setDf(temporaryElem.getDictionaryElem().getDf() + currentBlock.getDictionaryElem().getDf());
                    temporaryElem.getDictionaryElem().setCf(temporaryElem.getDictionaryElem().getCf() + currentBlock.getDictionaryElem().getCf());
                    readLineFromDocId(docPointers.get(blockNumber), currentBlock.getDictionaryElem().getLengthDocIds(), temporaryDocIds);
                    readLineFromFreq(freqPointers.get(blockNumber), currentBlock.getDictionaryElem().getLengthDocIds(), temporaryFreqs);

                    if (isEndOfFile(vocPointers.get(blockNumber))) {
                        continue;
                    }
                    // if the block is not finished, the next vocabulary entry is read and added to the priority queue
                    readSPIMIEntryFromDictionary(vocPointers.get(blockNumber), blockNumber, pQueueElems.get(blockNumber), termBytes);
                    pQueue.add(pQueueElems.get(blockNumber));
                }

                // set inverse document frequency of the temporaryElem
                temporaryElem.getDictionaryElem().setIdf();
                // set max term frequency of the temporaryElem
                for (int i = 0; i < temporaryFreqs.size(); i++) {
                    if (temporaryFreqs.get(i) > temporaryElem.getDictionaryElem().getMaxTF()) {
                        temporaryElem.getDictionaryElem().setMaxTF(temporaryFreqs.get(i));
                    }
                }
                // set max TFIDF of the temporaryElem
                temporaryElem.getDictionaryElem().computeMaxTFIDF();
                // for each document in the posting list of temporaryElem, open the docIndex file, retrieve the documentLength and compute the BM25
                for (int i = 0; i < temporaryDocIds.size(); i++) {
                    int docLength = docLengths[temporaryDocIds.get(i)];
                    temporaryElem.getDictionaryElem().computeMaxBM25(docLength);
                }


                // implement skipping
                if (temporaryDocIds.size() >= 1024) {
                    blockLength = (int) Math.ceil(Math.sqrt(temporaryDocIds.size()));
                    // for every blockLength docIds, create a skip element and add it to the skipList
                    for (int i = 0; i < temporaryDocIds.size(); i += blockLength) {
                        // the step is the number of bytes needed to store the block, it varies based on the compression
                        int docStep = blockLength * 4;
                        int freqStep = blockLength * 4;
                        // if compression is active use VariableByte to compress docids and Unary to compress frequencies
                        if (compression) {
                            byte[] blockDocs;
                            byte[] blockFreqs;
                            //the step is the number of bytes needed to store the block, it varies based on the docids and frequencies
                            blockDocs = VariableByte.fromArrayIntToVarByte(new ArrayList<Integer>(temporaryDocIds.subList(i, Math.min(i + blockLength, temporaryDocIds.size()))));
                            blockFreqs = Unary.ArrayIntToUnary(new ArrayList<Integer>(temporaryFreqs.subList(i, Math.min(i + blockLength, temporaryFreqs.size()))));
                            docStep = blockDocs.length;
                            freqStep = blockFreqs.length;
                            docsBuffer = docsFchan.map(FileChannel.MapMode.READ_WRITE, docOff, docStep);
                            freqsBuffer = freqsFchan.map(FileChannel.MapMode.READ_WRITE, freqOff, freqStep);
                            docsBuffer.put(blockDocs);
                            freqsBuffer.put(blockFreqs);

                        } else {
                            // if compression is not active, write the docIds and frequencies in the file, 4 bytes for each entry
                            docsBuffer = docsFchan.map(FileChannel.MapMode.READ_WRITE, docOff, blockLength * 4L);
                            freqsBuffer = freqsFchan.map(FileChannel.MapMode.READ_WRITE, freqOff, blockLength * 4L);
                            for (int j = 0; j < blockLength; j++) {
                                docsBuffer.putInt(temporaryDocIds.get(Math.min(i + j, temporaryDocIds.size() - 1)));
                                freqsBuffer.putInt(temporaryFreqs.get(Math.min(i + j, temporaryFreqs.size() - 1)));
                            }
                        }
                        // create the skip element and add it to the skipList
                        SkipElem skipElem = new SkipElem();
                        skipElem.setDocID(temporaryDocIds.get(Math.min(i + blockLength - 1, temporaryDocIds.size() - 1)));
                        skipElem.setOffsetDoc(docOff);
                        skipElem.setOffsetFreq(freqOff);
                        skipElem.setDocBlockLen(docStep);
                        skipElem.setFreqBlockLen(freqStep);
                        skipList.add(skipElem);
                        // update the offset of the docIds and frequencies
                        docOff += docStep;
                        freqOff += freqStep;
                    }
                    // write the skipList in the skip file
                    skipBuffer = skipFchan.map(FileChannel.MapMode.READ_WRITE, skipOff, (long) skipList.size() * SkipElem.size());
                    // call the method to write the skipList in the file
                    for (int i = 0; i < skipList.size(); i++) {
                        skipList.get(i).writeToFile(skipBuffer);
                    }
                    // update the statistics of the temporaryElem
                    temporaryElem.getDictionaryElem().setOffsetSkip(skipOff);
                    temporaryElem.getDictionaryElem().setSkipLen(skipList.size());
                    temporaryElem.getDictionaryElem().setLengthDocIds((int) (docOff - latestDocOff));
                    temporaryElem.getDictionaryElem().setLengthFreq((int) (freqOff - latestFreqOff));
                    // update the offset of the skip information
                    skipOff += (long) skipList.size() * SkipElem.size();
                    // clear the skipList
                    skipList.clear();

                } else { //skip not used
                    // if compression is active use VariableByte to compress docids and Unary to compress frequencies
                    if (compression) {
                        byte[] temporaryDocIdsBytes = VariableByte.fromArrayIntToVarByte((ArrayList<Integer>) temporaryDocIds);
                        byte[] temporaryFreqsBytes = Unary.ArrayIntToUnary((ArrayList<Integer>) temporaryFreqs);
                        //set the offset for the next term and open the buffers
                        docOff += temporaryDocIdsBytes.length;
                        freqOff += temporaryFreqsBytes.length;
                        docsBuffer = docsFchan.map(FileChannel.MapMode.READ_WRITE, latestDocOff, temporaryDocIdsBytes.length);
                        freqsBuffer = freqsFchan.map(FileChannel.MapMode.READ_WRITE, latestFreqOff, temporaryFreqsBytes.length);
                        docsBuffer.put(temporaryDocIdsBytes);
                        freqsBuffer.put(temporaryFreqsBytes);
                        //if compression is set lenght is the bytes size of their entry
                        temporaryElem.getDictionaryElem().setLengthDocIds(temporaryDocIdsBytes.length);
                        temporaryElem.getDictionaryElem().setLengthFreq(temporaryFreqsBytes.length);

                    } else {
                        docOff += temporaryElem.getDictionaryElem().getLengthDocIds() * 4L;
                        freqOff += temporaryElem.getDictionaryElem().getLengthDocIds() * 4L;
                        docsBuffer = docsFchan.map(FileChannel.MapMode.READ_WRITE, latestDocOff, temporaryDocIds.size() * 4L);
                        freqsBuffer = freqsFchan.map(FileChannel.MapMode.READ_WRITE, latestFreqOff, temporaryFreqs.size() * 4L);
                        for (int i = 0; i < temporaryElem.getDictionaryElem().getLengthDocIds(); i++) {
                            docsBuffer.putInt(temporaryDocIds.get(i));
                            freqsBuffer.putInt(temporaryFreqs.get(i));

                        }
                        temporaryElem.getDictionaryElem().setLengthDocIds(temporaryDocIds.size());
                        temporaryElem.getDictionaryElem().setLengthFreq(temporaryFreqs.size());
                    }
                    // if the block is too small, the skipList is empty
                    temporaryElem.getDictionaryElem().setOffsetSkip(0);
                    temporaryElem.getDictionaryElem().setSkipLen(0);
                }

                // update the offset of the docIds and frequencies
                temporaryElem.getDictionaryElem().setOffsetFreq(latestFreqOff);
                temporaryElem.getDictionaryElem().setOffsetDoc(latestDocOff);
                // write the vocabulary entry in the vocabulary file
                vocBuffer = vocabularyFchan.map(FileChannel.MapMode.READ_WRITE, termNumber * DictionaryElem.size(), DictionaryElem.size());
                termNumber++;
                temporaryElem.getDictionaryElem().writeElemToDisk(vocBuffer);
                temporaryDocIds.clear();
                temporaryFreqs.clear();

            } // end while

        } catch (Exception e) {
            e.printStackTrace();
        }

        // save the collection length and the average doc length in a file
        Files.write(Paths.get(Global.collectionInfoFile), (collectionLength + "\n" + Global.averageDocLength + "\n" + Global.compression + "\n" + Global.stopWords + "\n" + Global.stem).getBytes());

    }

    // method to read an entry from a vocabulary using the input array for the term and writing the entry in the input termBlock
    public static void readSPIMIEntryFromDictionary(RandomAccessFile raf, int n, termBlock readBlock, byte[] termBytes) throws IOException {
        // Read 40 bytes into a byte array
        ByteBuffer vocBuffer = ByteBuffer.allocate(DictionaryElem.SPIMIsize());
        while (vocBuffer.hasRemaining()) {
            raf.getChannel().read(vocBuffer);
        }
        vocBuffer.rewind();
        vocBuffer.get(termBytes, 0, 40);

        // Read next 4 bytes into an int
        int int1 = vocBuffer.getInt();

        // Read next 4 bytes into an int
        int int2 = vocBuffer.getInt();

        // Read next 8 bytes into a long
        long long1 = vocBuffer.getLong();

        // Read next 4 bytes into an int
        int int3 = vocBuffer.getInt();

        readBlock.setNumBlock(n);
        readBlock.getDictionaryElem().setTerm(SPIMI.decodeTerm(termBytes));
        readBlock.getDictionaryElem().setDf(int1);
        readBlock.getDictionaryElem().setCf(int2);
        readBlock.getDictionaryElem().setOffsetDoc(long1);
        readBlock.getDictionaryElem().setOffsetFreq(long1);
        readBlock.getDictionaryElem().setLengthDocIds(int3);
        readBlock.getDictionaryElem().setLengthFreq(int3);
        //readBlock.getDictionaryElem().printDebug();
    }

    // method to read a line of docIds from a file and write it in the input list
    public static void readLineFromDocId(RandomAccessFile raf, int length, List<Integer> ids) throws IOException {
        ByteBuffer docsIdsBuffer = ByteBuffer.allocate(length * 4);
        while (docsIdsBuffer.hasRemaining()) {
            raf.getChannel().read(docsIdsBuffer);
        }
        docsIdsBuffer.rewind();
        for (int i = 0; i < length; i++) {
            ids.add(docsIdsBuffer.getInt());
        }
    }

    // method to read a line of frequencies from a file and write it in the input list
    public static void readLineFromFreq(RandomAccessFile raf, int length, List<Integer> freqs) throws IOException {
        ByteBuffer freqsBuffer = ByteBuffer.allocate(length * 4);
        while (freqsBuffer.hasRemaining()) {
            raf.getChannel().read(freqsBuffer);
        }
        freqsBuffer.rewind();
        for (int i = 0; i < length; i++) {
            freqs.add(freqsBuffer.getInt());
        }
    }

    // method to check if the pointer of a file is at the end of the file
    public static boolean isEndOfFile(RandomAccessFile raf) throws IOException {
        return raf.getFilePointer() == raf.length();
    }


}
