package org.commoncrawl.mklab.analysis;

import com.google.common.collect.Multiset;
import com.google.common.collect.Multisets;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.stream.JsonReader;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.util.zip.GZIPInputStream;

/**
 * Created by kandreadou on 10/23/14.
 */
public class CommonCrawlAnalyzer {

    private Gson gson = new Gson();
    private static int JSON_SYNTAX_PROBLEM_COUNT = 0;

    private static int NEWS_COUNT = 0;
    private static int DOM_SIB = 0;
    private static int DOM_DEPTH = 0;
    public static Set<String> STRINGS = new HashSet<String>();
    public static Set<String> CASES = new HashSet<String>();

    ProcessingService service = new ProcessingService();
    static long start = System.currentTimeMillis();

    public static void main(String[] args) throws Exception {


        MorphiaManager.setup("cc_combine");
        CommonCrawlAnalyzer a = new CommonCrawlAnalyzer();
        //a.readDomainsFromFile();
        long start = System.currentTimeMillis();
        a.analyzeCommonCrawlLocal();
        Statistics.printStatistics();
        MorphiaManager.tearDown();

        System.out.println("Counter "+ImageVectorization.counter);
        System.out.println("SCALING " + ImageVectorization.SCALING / ImageVectorization.counter);
        System.out.println("FEATURES " + ImageVectorization.FEATURE_EXTRACTION / ImageVectorization.counter);
        System.out.println("CLASSIFY " + ImageVectorization.CLASSIFY / ImageVectorization.counter);
        System.out.println("VLAD & PCA " + ImageVectorization.VALD_PCA / ImageVectorization.counter);
        System.out.println("INDEX " + IndexingManage.INDEXING / ImageVectorization.counter);
        System.out.print("Duration "+ ((System.currentTimeMillis()-start)/1000)+" seconds");
        System.out.println("Total pure time "+ImageVectorization.TOTAL_TIME/1000);
        /*System.out.println("WEBPAGES");
        Iterable<Multiset.Entry<String>> webPagesSetSortedByCount =
                Multisets.copyHighestCountFirst(Statistics.NEWS_WEBPAGES_FREQUENCIES).entrySet();
        for (Multiset.Entry<String> s : webPagesSetSortedByCount) {
            System.out.println("Frequency: " + s.getElement() + " " + s.getCount());
        }
        System.out.println("IMAGES");
        Iterable<Multiset.Entry<String>> imageSetSortedByCount =
                Multisets.copyHighestCountFirst(Statistics.NEWS_IMAGES_FREQUENCIES).entrySet();
        int newsImageCount = 0;
        for (Multiset.Entry<String> s : imageSetSortedByCount) {
            newsImageCount += s.getCount();
            System.out.println("Frequency: " + s.getElement() + " " + s.getCount());
        }
        System.out.println("VIDEOS");
        Iterable<Multiset.Entry<String>> videoSetSortedByCount =
                Multisets.copyHighestCountFirst(Statistics.NEWS_VIDEO_FREQUENCIES).entrySet();
        int newsVideoCount = 0;
        for (Multiset.Entry<String> s : videoSetSortedByCount) {
            newsVideoCount += s.getCount();
            System.out.println("Frequency: " + s.getElement() + " " + s.getCount());
        }

        long duration = System.currentTimeMillis() - start;
        System.out.println("UNIQUE URLS: " + Statistics.GLOBAL_COUNT);
        System.out.println("UNIQUE DOMAINS: " + Statistics.DOMAIN_COUNT);
        System.out.println("NEWS IMAGE COUNT: " + newsImageCount);
        System.out.println("NEWS VIDEO COUNT: " + newsVideoCount);
        System.out.println("Total time in millis: " + duration / 1000 + " seconds");*/
    }

    protected void processLineSync(String jsonline) {
        try {
            JsonReader reader = new JsonReader(new StringReader(jsonline));
            reader.setLenient(true);
            CrawledImage b = gson.fromJson(reader, CrawledImage.class);
            if (b != null) {
                URLProcessor p = new URLProcessor(b.src, b.pageUrl);
                URLProcessor.Result r = p.call();
            }
        } catch (JsonSyntaxException je) {
            JSON_SYNTAX_PROBLEM_COUNT++;
        }
    }

    protected void processLine(String jsonline) {
        try {
            JsonReader reader = new JsonReader(new StringReader(jsonline));
            reader.setLenient(true);
            CrawledImage b = gson.fromJson(reader, CrawledImage.class);
            if (b != null) {
                boolean lineConsumed = false;
                while (!lineConsumed) {
                    if (service.canAcceptMoreTasks()) {
                        service.submitTask(b);
                        lineConsumed = true;
                    }
                    /*else {
                        service.printStatus();
                    }*/
                    ProcessingService.Result r = service.tryGetResult();
                    while (r!=null) {
                        //System.out.println("Image url: " + r.url);
                        // System.out.println("Content length: " + r.contentLength);
                        //System.out.println("Content type: " + r.contentType);

                        /*if (r != null && r.success) {

                            handleURL(r.url, r.pageUrl);
                        } else {
                            if (r != null)
                                System.out.println("Failed " + r.url);
                        }*/
                        r = service.tryGetResult();
                    }
                }

            }
            reader.close();
            reader = null;
        } catch (JsonSyntaxException je) {
            JSON_SYNTAX_PROBLEM_COUNT++;
            //System.out.println(je);
            //System.out.println(jsonline);
            //System.out.println("#NUM EXCEPTIONS "+JSON_SYNTAX_PROBLEM_COUNT);
        } catch (IOException e) {

        }
    }


    protected void analyzeCommonCrawlLocal() throws IOException {
        //FILE FOR TRAINING
        //File file = new File("/home/kandreadou/Music/commoncrawl/July2014_40001_end/output41001/");
        //FILE FOR TESTING
        //File file = new File("/home/kandreadou/Music/commoncrawl/July2014_40001_end/output55501/");
        //FILE FOR COMBINED APPROACH
        File file = new File("/home/kandreadou/Music/commoncrawl/July2014_40001_end/output49001/");
        readRecursivelyInLocalFolder(file);
        service.shutDown();
    }

    //int numMax = 20000;

    protected void readRecursivelyInLocalFolder(File folder) throws IOException {
        for (final File fileEntry : folder.listFiles()) {
            //if (ImageVectorization.counter >= numMax)
                //return;
            Statistics.printStatistics();
            System.out.println("Total time: " + (System.currentTimeMillis() - start) / 1000);
            if (fileEntry.isDirectory()) {
                readRecursivelyInLocalFolder(fileEntry);
            } else {
                System.out.println("Reading from file: " + fileEntry.getPath());
                readFromZipFile(fileEntry);
            }
        }
    }

    protected void readFromZipFile(File file) throws IOException {
        if (file.getPath().endsWith(".gz")) {
            //processFile(file);
            BufferedReader reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(file)), "UTF-8"));
            String line = reader.readLine();
            while (line != null){ //&& ImageVectorization.counter<numMax) {
                processLine(line);
                line = reader.readLine();
            }
            reader.close();
            reader = null;
        }
    }

    protected void readDomainsFromFile() throws IOException {
        InputStream configStream = new FileInputStream("/home/kandreadou/Desktop/news_img_domains.txt");
        Scanner scanner = new Scanner(configStream);
        while (scanner.hasNextLine()) {
            String line = scanner.nextLine();
            STRINGS.add(line);
        }
        scanner.close();
        configStream.close();

        configStream = new FileInputStream("/home/kandreadou/Desktop/cases.txt");
        scanner = new Scanner(configStream);
        while (scanner.hasNextLine()) {
            String line = scanner.nextLine();
            CASES.add(line);
        }
        scanner.close();
        configStream.close();
    }
}
