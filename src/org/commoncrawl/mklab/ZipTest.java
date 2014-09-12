package org.commoncrawl.mklab;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Scanner;
import java.util.zip.GZIPInputStream;

/**
 * Created by kandreadou on 9/10/14.
 */
public class ZipTest {

    public static void main(String[] args) throws Exception {
        //String INPUT_ZIP_FILE = "/home/kandreadou/Downloads/part-r-00000.gz";
        //String OUTPUT_FOLDER = "/home/kandreadou/Downloads/testunzip.txt";
        ZipTest unZip = new ZipTest();
        //unZip.unZipIt(INPUT_ZIP_FILE,OUTPUT_FOLDER);

        /*String start = "s3://aws-publicdatasets/common-crawl/crawl-data/CC-MAIN-2014-23/segments/1404776400583.60/warc/CC-MAIN-20140707234000-0000";
        String end = "-ip-10-180-212-248.ec2.internal.warc.gz";
        String inputPath="";
        for (int i=0; i<10; i++){
            inputPath+=start+i+end;
            if(i!=9)
                inputPath+=',';
        }
        System.out.println(inputPath);*/

        unZip.addPaths(10);

    }

    private void addPaths(int numPath) {
        int counter = 0;
        InputStream configStream = getClass().getResourceAsStream("/warc.path");
        Scanner scanner = new Scanner(configStream);
        while (scanner.hasNextLine() && counter < numPath) {
            counter++;
            System.out.println("s3n://aws-publicdatasets/" + scanner.nextLine());
            //FileInputFormat.addInputPath(job, new org.apache.hadoop.fs.Path("s3n://aws-publicdatasets/" + scanner.nextLine()));
        }

    }


    public void unZipIt(String zipFile, String outputFile) {

        byte[] buffer = new byte[1024];

        try {

            GZIPInputStream gzis =
                    new GZIPInputStream(new FileInputStream(zipFile));

            FileOutputStream out =
                    new FileOutputStream(outputFile);

            int len;
            while ((len = gzis.read(buffer)) > 0) {
                out.write(buffer, 0, len);
            }

            gzis.close();
            out.close();

            System.out.println("Done");

        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
}
