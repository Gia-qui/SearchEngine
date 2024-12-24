package it.unipi.mrcv;

import it.unipi.mrcv.data_structures.Document;
import it.unipi.mrcv.global.Global;
import it.unipi.mrcv.index.fileUtils;
import it.unipi.mrcv.preprocess.preprocess;
import it.unipi.mrcv.query.ConjunctiveQuery;
import it.unipi.mrcv.query.DAAT;
import it.unipi.mrcv.query.MaxScore;

import java.io.*;
import java.util.PriorityQueue;

public class CLI {
    private enum ScoringMethod {
        TF_IDF, BM25
    }

    private enum QueryMode {
        DAAT, MAXSCORE, CONJUNCTIVE
    }

    public static void main(String[] args) throws IOException {
        Global.indexing = false;
        Global.load();

        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        RandomAccessFile raf = new RandomAccessFile(new File(Global.prefixDocIndex), "r");
        ScoringMethod scoringMethod = null;
        QueryMode queryMode = null;
        int numResults = 10; // Default number of results
        boolean setNumResults = false;

        while (true) {
            if (scoringMethod == null) {
                System.out.println("Choose scoring method: [1] TF-IDF, [2] BM25");
                String input = reader.readLine();
                if ("1".equals(input)) {
                    scoringMethod = ScoringMethod.TF_IDF;
                    Global.isBM25 = false;
                } else if ("2".equals(input)) {
                    scoringMethod = ScoringMethod.BM25;
                    Global.isBM25 = true;
                }
            } else if (queryMode == null) {
                System.out.println("Choose query mode: [1] DAAT, [2] MAXSCORE, [3] CONJUNCTIVE");
                System.out.println("Type '_return' to go back.");
                String input = reader.readLine();
                if ("_return".equals(input)) {
                    scoringMethod = null;
                    continue;
                }
                switch (input) {
                    case "1":
                        queryMode = QueryMode.DAAT;
                        break;
                    case "2":
                        queryMode = QueryMode.MAXSCORE;
                        break;
                    case "3":
                        queryMode = QueryMode.CONJUNCTIVE;
                        break;
                }
            } else {
                if (!setNumResults) {
                    System.out.println("Enter the number of results you want: (Type '_return' to choose a different query mode)");
                    String numResultsInput = reader.readLine();
                    if ("_return".equals(numResultsInput)) {
                        queryMode = null;
                        continue;
                    }
                    try {
                        numResults = Integer.parseInt(numResultsInput);
                    } catch (NumberFormatException e) {
                        System.out.println("Invalid input. Using default number of results: 10");
                        numResults = 10;
                    }
                    setNumResults = true;
                }

                System.out.println("Enter your query: (Type '_return' to change the number of results)");
                String query = reader.readLine();
                if ("_return".equals(query)) {
                    setNumResults = false; // Allow changing number of results
                    continue;
                }

                long startTime = System.currentTimeMillis();

                PriorityQueue<Document> results = null;
                switch (queryMode) {
                    case DAAT:
                        results = DAAT.executeDAAT(preprocess.all(query), numResults);
                        break;
                    case MAXSCORE:
                        results = MaxScore.executeMaxScore(preprocess.all(query), numResults);
                        break;
                    case CONJUNCTIVE:
                        results = ConjunctiveQuery.executeConjunctiveQuery(preprocess.all(query), numResults);
                        break;
                }

                long endTime = System.currentTimeMillis();
                long duration = endTime - startTime;

                if (results != null && !results.isEmpty()) {
                    System.out.println("Query processed in " + duration + " milliseconds.");
                    while (!results.isEmpty()) {
                        Document doc = results.poll();
                        System.out.println("DocNumber: " + fileUtils.getDocNumber(doc.getDocId(), raf) + " | Score: " + doc.getScore());
                    }
                } else {
                    System.out.println("No results found. Query processed in " + duration + " milliseconds.");
                }
            }
        }
    }
}
