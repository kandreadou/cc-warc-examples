package org.commoncrawl.mklab.analysis;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.stream.JsonReader;
import jcifs.smb.NtlmPasswordAuthentication;
import jcifs.smb.SmbFile;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.zip.GZIPInputStream;

/**
 * Created by kandreadou on 9/10/14.
 */
public class ZipTest {

    private Gson gson = new Gson();
    private static String user = "kandreadou";
    private static String pass = "";
    private static final NtlmPasswordAuthentication auth = new NtlmPasswordAuthentication("", user, pass);
    int counter = 0;
    private static List<String> STRINGS = new ArrayList<String>();

    public static void main(String[] args) throws Exception {
        //String INPUT_ZIP_FILE = "/home/kandreadou/Downloads/part-r-00000.gz";
        //String OUTPUT_FOLDER = "/home/kandreadou/Downloads/testunzip.txt";

        ZipTest unZip = new ZipTest();
        unZip.readDomainsFromFile();
        unZip.analyzeCommonCrawlLocal();


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

        //unZip.readFromZipFile("/media/kandreadou/New Volume/1_COMMON_CRAWL/20140915_1133/part-r-00000.gz", 200);
        //unZip.addPaths(550, 555);
        //unZip.createJob();

    }

    private void createJob() {
        int start = 2101;
        int end = 3100;
        boolean success = true;

        while (start < end) {
            int intermediate = start + 100;
            if (intermediate > end)
                intermediate = end;
            System.out.println("start " + start + " start+100 " + intermediate);
            start += 100;
        }
    }

    private void analyzeCommonCrawl() throws Exception{
        String path = "smb://160.40.51.9/mklab-dataset-repo-1/kandr/commoncrawl/July2014_0_100/";

        SmbFile smbFile = new SmbFile(path, auth);

        ZipTest unZip = new ZipTest();

        unZip.readRecursivelyInFolder(smbFile);
    }

    private void analyzeCommonCrawlLocal() throws Exception{
        String path = "/home/kandreadou/Documents/todo/";

        File smbFile = new File(path);

        ZipTest unZip = new ZipTest();

        unZip.readRecursivelyInLocalFolder(smbFile);
    }

    private void addPaths(int start, int end) {
        int counter = 0;
        InputStream configStream = getClass().getResourceAsStream("/warc.path");
        Scanner scanner = new Scanner(configStream);
        while (scanner.hasNextLine() && counter < end) {
            counter++;
            String line = scanner.nextLine();
            if (counter >= start)
                System.out.println("s3n://aws-publicdatasets/" + line);
            //FileInputFormat.addInputPath(job, new org.apache.hadoop.fs.Path("s3n://aws-publicdatasets/" + scanner.nextLine()));

        }
    }

    public void readRecursivelyInLocalFolder(File folder) throws IOException {
        for (final File fileEntry : folder.listFiles()) {
            if (fileEntry.isDirectory()) {
                readRecursivelyInLocalFolder(fileEntry);
            } else {
                System.out.println("Reading from file: " + fileEntry.getPath());
                readFromZipFile(fileEntry.getPath());
            }
        }
    }

    public void readRecursivelyInFolder(SmbFile folder) throws IOException {
        for (final SmbFile fileEntry : folder.listFiles()) {
            if (fileEntry.isDirectory()) {
                readRecursivelyInFolder(fileEntry);
            } else {
                System.out.println("Reading from file: " + fileEntry.getPath());
                readFromZipFile(fileEntry.getPath());
            }
        }
    }

    public void readDomainsFromFile() throws Exception {
        InputStream configStream = new FileInputStream("/home/kandreadou/workspace/bubing-0.9.3/seeds.txt");
        Scanner scanner = new Scanner(configStream);
        while (scanner.hasNextLine()) {
            try {
                URL url = new URL(scanner.nextLine());
                String host = url.getHost();
                if (host.startsWith("www.")) {
                    host = host.substring(4);
                }
                STRINGS.add(host);
            }catch(MalformedURLException m){

            }
        }
    }

    private static int GLOBAL_COUNT=0;
    private static int NEWS_COUNT=0;
    private static int DOM_SIB=0;
    private static int DOM_DEPTH=0;

    public void processLine(String jsonline){
        try {
            JsonReader reader = new JsonReader(new StringReader(jsonline));
            reader.setLenient(true);
            B b = gson.fromJson(reader, B.class);
            if(b!=null) {
                GLOBAL_COUNT++;
                DOM_SIB += b.domSib;
                DOM_DEPTH += b.domDepth;
                for (String s : STRINGS) {
                    if (b.src.contains(s)) {
                        NEWS_COUNT++;
                        System.out.println("New site url found: " + b.src + " " + b.pageUrl);
                        System.out.println("GLOBAL COUNT: " + GLOBAL_COUNT + " NEWS COUNT " + NEWS_COUNT);
                        System.out.println("Average dom siblings :" + DOM_SIB / GLOBAL_COUNT);
                        System.out.println("Average dom depth :" + DOM_DEPTH / GLOBAL_COUNT);
                    }
                }
            }
        }catch(JsonSyntaxException je){
            System.out.println(je);
            System.out.println(jsonline);
        }
    }

    public void readFromZipFile(String file) throws IOException {
        if (file.endsWith(".gz")) {
            //SmbFile smbFile = new SmbFile(file, auth);
            //Scanner scanner = new Scanner(new GZIPInputStream(new SmbFileInputStream(smbFile)));
            File myFile = new File(file);
            Scanner scanner = new Scanner(new GZIPInputStream(new FileInputStream(myFile)));
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                processLine(line);

            }
        }
        System.out.println("GLOBAL COUNT: "+GLOBAL_COUNT+" NES COUNT "+NEWS_COUNT);
        System.out.println("Average dom siblings :"+DOM_SIB/GLOBAL_COUNT);
        System.out.println("Average dom depth :"+DOM_DEPTH/GLOBAL_COUNT);
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

    /**
     * {"src":"\n\t\t\t\t\t\t\t\thttp://s7ondemand1.scene7.com/is/image/MoosejawMB/10211640x1073912_zm?$thumb150$\n\t\t\t\t\t\t\t\t",
     "alt":"ExOfficio Women\u0027s Crossback Diamond Dress",
     "w":"",
     "h":"",
     "pageUrl":"http://www.moosejaw.com/moosejaw/shop/product_Isis-Women-s-Aida-Dress_10227940_10208_10000001_-1_",
     "parentTxt":"ExOfficio Women\u0027s Crossback Diamond Dress",
     "domSib":4,
     "domDepth":23,
     "domElem":"img"},

     * @return
     */
    class B{
        public String src, alt, pageUrl, parentTxt, img;
        public int domSib,domDepth;

    }
}
