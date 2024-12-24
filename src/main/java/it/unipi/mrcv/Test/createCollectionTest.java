package it.unipi.mrcv.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.PriorityQueue;
import java.util.zip.GZIPOutputStream;

import it.unipi.mrcv.data_structures.Document;
import it.unipi.mrcv.global.Global;
import it.unipi.mrcv.index.Merger;
import it.unipi.mrcv.index.SPIMI;
import it.unipi.mrcv.index.fileUtils;
import it.unipi.mrcv.preprocess.preprocess;
import it.unipi.mrcv.query.ConjunctiveQuery;
import it.unipi.mrcv.query.DAAT;
import it.unipi.mrcv.query.MaxScore;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.utils.IOUtils;

public class createCollectionTest {
    public static void main(String[] args) throws IOException, InterruptedException {
        fileUtils.deleteTempFiles();
        fileUtils.deleteFilesCompressed();
        fileUtils.deleteFiles();

        // delete the old collection
        File filetxt = new File("collection_prova.txt");
        File filetar = new File("data.tar.gz");
        filetxt.delete();
        filetar.delete();
        Global.indexing = true;
        Global.load();
        ArrayList<String> terms = new ArrayList<>();
        terms.add("gggg");
        terms.add("hhhh");
        terms.add("tttt");
        terms.add("bbbb");
        terms.add("aaaa");
        terms.add("zzzz");
        terms.add("dddd");
        terms.add("cccc");
        terms.add("eeee");
        terms.add("ffff");
        ArrayList<String> lines = new ArrayList<>();
        // create the collection
        for(int i=0; i<terms.size(); i++) {
            for(int j=i;j<terms.size(); j++) {
                for(int k=j;k<terms.size();k++) {
                    int docn = i * terms.size() * terms.size() + j * terms.size() + k;
                    String line = docn + "\t" +terms.get(i) + " " + terms.get(j) + " " + terms.get(k);
                    lines.add(line);
                }
            }

        }
        // write the collection to a file
        FileWriter fileWriter = null;

        fileWriter = new FileWriter("test_collection.tsv");

        BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);

        for (String item : lines) {
            bufferedWriter.write(item);
            bufferedWriter.newLine();
        }

        bufferedWriter.close();
        fileWriter.close();

        // compress the collection
        // Specify the path to the .txt file you want to compress
        String txtFilePath = "test_collection.tsv";

        // Specify the path for the .tar.gz file
        String tarGzFilePath = "data.tar.gz";

        try {
            FileOutputStream fos = new FileOutputStream(tarGzFilePath);
            TarArchiveOutputStream tarOS = new TarArchiveOutputStream(new GZIPOutputStream(fos));

            // Create a Tar entry for the .txt file
            TarArchiveEntry entry = new TarArchiveEntry(new File(txtFilePath), "test_collection.tsv");

            // Put the Tar entry
            tarOS.putArchiveEntry(entry);

            // Read the .txt file and write it to the Tar entry
            FileInputStream fis = new FileInputStream(txtFilePath);
            IOUtils.copy(fis, tarOS);

            // Close the Tar entry
            tarOS.closeArchiveEntry();

            // Close the Tar output stream
            tarOS.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        SPIMI.exeSPIMI("data.tar.gz");
        Merger.Merge();
        Global.indexing=false;
        Global.load();
        //test the queries on the collection
        for(int i=0; i<lines.size(); i++) {
            String line=lines.get(i);
            String[] parts = line.split("\t", 2);
            ArrayList<String> query = preprocess.all(parts[1]);
            PriorityQueue<Document> Results = new PriorityQueue<>();
            //DAAT
            Results=DAAT.executeDAAT(query, 10);
            assert Results.poll().getDocId()==i : "DAAT: docId " + i + " not in results";
            //MAXSCORE
            Results=MaxScore.executeMaxScore(query, 10);
            assert Results.poll().getDocId()==i : "MAXSCORE: docId " + i + " not in results";
            //CONJUNCTIVE
            Results=ConjunctiveQuery.executeConjunctiveQuery(query, 10);
            assert Results.poll().getDocId()==i : "CONJUNCTIVE: docId " + i + " not in results";
        }
        System.out.println("Test passed on the test collection");

    }
}
