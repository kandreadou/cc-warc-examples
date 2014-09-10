package org.commoncrawl.mklab;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;

/**
 * Created by kandreadou on 9/10/14.
 */
public class ZipTest {

    public static void main( String[] args )
    {
        String INPUT_ZIP_FILE = "/home/kandreadou/Downloads/part-r-00000.gz";
        String OUTPUT_FOLDER = "/home/kandreadou/Downloads/testunzip.txt";
        ZipTest unZip = new ZipTest();
        unZip.unZipIt(INPUT_ZIP_FILE,OUTPUT_FOLDER);
    }


    public void unZipIt(String zipFile, String outputFile) {

        byte[] buffer = new byte[1024];

        try{

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

        }catch(IOException ex){
            ex.printStackTrace();
        }
    }
}
