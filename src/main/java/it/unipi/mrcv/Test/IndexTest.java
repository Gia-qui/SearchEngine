package it.unipi.mrcv.Test;

import it.unipi.mrcv.data_structures.DictionaryElem;
import it.unipi.mrcv.data_structures.Posting;
import it.unipi.mrcv.data_structures.PostingList;
import it.unipi.mrcv.global.Global;
import it.unipi.mrcv.preprocess.preprocess;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class IndexTest {
    //static method that receives in input the path of the file containing the terms, retrieve the posting list for each term and check if the posting list is correct
    public static void testPostingLists(String path) {
        try {
            List<String> terms = null;

            terms = Files.readAllLines(Paths.get(path));
            //preprocess the terms
            terms = preprocess.all(String.join(" ", terms));
            ArrayList<PostingList> postingLists = new ArrayList<>();
            ArrayList<DictionaryElem> dictionaryElems = new ArrayList<>();
            ArrayList<String> termsToRemove = new ArrayList<>();
            for (String term : terms) {
                DictionaryElem elem = DictionaryElem.binarySearch(term);
                if (elem != null) {
                    PostingList pl = new PostingList(elem);
                    postingLists.add(pl);
                    dictionaryElems.add(elem);
                } else {
                    termsToRemove.add(term);
                }
            }
            for (String term : termsToRemove) {
                terms.remove(term);
            }
            System.out.println("Posting lists created");
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("collection.tsv"), StandardCharsets.UTF_8));
            String line=reader.readLine();
            String documentNumber;
            int docId=0;
            while(line!=null) {
                String[] parts = line.split("\t", 2);
                try {
                    int number = Integer.parseInt(parts[0]);
                    // document number is saved as string with 7 digits
                    documentNumber = String.format("%07d", number);
                } catch (NumberFormatException e) {
                    System.err.println("The first part is not an integer. Exiting.");
                    System.exit(1);
                }
                ArrayList<String> tokens = preprocess.all(parts[1]);
                ArrayList<Integer> frequencies = new ArrayList<>();
                for(int i=0; i<dictionaryElems.size(); i++) {
                    frequencies.add(0);
                }
                for (String term : tokens) {
                    if (term.length() == 0) {
                        continue;
                    }
                    //check if the term is in the dictionaryElems
                    int index = terms.indexOf(term);
                    if (index == -1){
                        continue;
                    }
                    frequencies.set(index, frequencies.get(index) + 1);
                }
                for(int i=0; i<dictionaryElems.size(); i++) {
                    if(frequencies.get(i)!=0) {
                        Posting pNew = new Posting(docId, frequencies.get(i));
                        Posting pOld = postingLists.get(i).getCurrent();
                        assert pOld.equals(pNew) : "Posting list for term "+dictionaryElems.get(i).getTerm()+" is not correct";
                        postingLists.get(i).next();
                    }
                }
                docId++;
                line=reader.readLine();

            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

        //static method that reads a list of terms from a file and returns a list of posting lists

    //main method that calls the testPostingLists method
    public static void main(String[] args) {
        Global.load();
        testPostingLists("terms.txt");
        System.out.println("Posting lists from terms.txt are correct");

    }

}
