package it.unipi.mrcv;

import it.unipi.mrcv.global.Global;
import it.unipi.mrcv.index.Merger;
import it.unipi.mrcv.index.SPIMI;
import it.unipi.mrcv.index.fileUtils;

import java.io.IOException;


public class Main {
    public static void main(String[] args) throws IOException, InterruptedException {
        // check the number of arguments and if -i,-c,-s are present
        // NOTE: these flags are used only for the indexing phase, if the program is started without them
        // then enters queryMode and the flag of the collection are loaded from the collectionInfo.txt file
        for (String arg : args) {
            if (arg.equals("-i")) {
                Global.indexing = true;
            }
            if (arg.equals("-c")) {
                Global.compression = true;
            }
            if (arg.equals("-s")) {
                Global.stem = true;
            }
            if (arg.equals("-sw")) {
                Global.stopWords = true;
            }
        }
        Global.load();
        if (Global.indexing) {
            System.out.println("Indexing");
            // remove old files
            fileUtils.deleteTempFiles();
            if (Global.compression)
                fileUtils.deleteFilesCompressed();
            else
                fileUtils.deleteFiles();
            long startTime = System.currentTimeMillis(); // Capture start time
            SPIMI.exeSPIMI("collection.tar.gz");
            Merger.Merge();
            long endTime = System.currentTimeMillis(); // Capture end time
            // Calculate the elapsed time and convert it to minutes
            long elapsedTimeMinutes = (endTime - startTime) / 1000 / 60;
            // Print the execution time in minutes
            System.out.printf("Execution time: " + elapsedTimeMinutes + " minutes\n");
            System.out.println("Indexing completed");
            // reload the global variables from the collectionInfo.txt file
        }

        // Query mode
        CLI.main(null);

    }
}
