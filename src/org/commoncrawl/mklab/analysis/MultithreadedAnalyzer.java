package org.commoncrawl.mklab.analysis;

import com.google.common.collect.Multiset;
import com.google.common.collect.Multisets;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.stream.JsonReader;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.GZIPInputStream;

/**
 * Created by kandreadou on 11/5/14.
 * NOTE: Not actively used. Testing.
 */
public class MultithreadedAnalyzer {

    public interface ThreadCompleteListener {
        void notifyOfThreadComplete();
    }

    private final ExecutorService executor = Executors.newFixedThreadPool(4);

    private Gson gson = new Gson();
    private static int JSON_SYNTAX_PROBLEM_COUNT = 0;

    private static int NEWS_COUNT = 0;
    private static int DOM_SIB = 0;
    private static int DOM_DEPTH = 0;
    public static Set<String> STRINGS = new HashSet<String>();
    ProcessingService service = new ProcessingService();
    int count = 0;

    public static void main(String[] args) throws Exception {

        MultithreadedAnalyzer a = new MultithreadedAnalyzer();
        a.readDomainsFromFile();
        a.analyzeCommonCrawlLocal();

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
                    } else {
                        service.printStatus();
                    }
                    ProcessingService.Result r = service.tryGetResult();
                    while (r != null) {
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
        } catch (JsonSyntaxException je) {
            JSON_SYNTAX_PROBLEM_COUNT++;
            //System.out.println(je);
            //System.out.println(jsonline);
            //System.out.println("#NUM EXCEPTIONS "+JSON_SYNTAX_PROBLEM_COUNT);
        }
    }

    int threadCount = 2;

    protected void analyzeCommonCrawlLocal() throws IOException {
        final long start = System.currentTimeMillis();

        ThreadCompleteListener listener = new ThreadCompleteListener() {
            @Override
            public void notifyOfThreadComplete() {
                threadCount--;
                if (threadCount > 0)
                    return;
                service.shutDown();
                System.out.println("WEBPAGES");
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
                System.out.println("Total time in millis: " + duration / 1000 + " seconds");
            }
        };
        new Thread(new AnalyzerRunnable("/home/kandreadou/Documents/todo/output30001/", listener)).start();
        new Thread(new AnalyzerRunnable("/home/kandreadou/Documents/todo/output30501/", listener)).start();

    }

    class AnalyzerRunnable implements Runnable {

        private String path;
        private ThreadCompleteListener listener;

        public AnalyzerRunnable(String filepath, ThreadCompleteListener listener) {
            this.path = filepath;
            this.listener = listener;
        }

        @Override
        public void run() {
            try {
                File file = new File(path);
                readRecursivelyInLocalFolder(file);
            } catch (Exception ex) {
                System.out.println("Exception in run: " + ex);
            } finally {
                listener.notifyOfThreadComplete();
            }
        }
    }

    protected void readRecursivelyInLocalFolder(File folder) throws IOException {
        for (final File fileEntry : folder.listFiles()) {
            if (count >= 2000000)
                return;
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
            Scanner scanner = new Scanner(new GZIPInputStream(new FileInputStream(file)), "UTF-8");
            while (scanner.hasNextLine() && count < 2000000) {
                count++;
                String line = scanner.nextLine();
                processLine(line);
            }
        }
    }

    protected void readDomainsFromFile() throws FileNotFoundException {
        InputStream configStream = new FileInputStream("/home/kandreadou/workspace/bubing-0.9.3/seeds.txt");
        Scanner scanner = new Scanner(configStream);
        while (scanner.hasNextLine()) {
            String line = scanner.nextLine();
            try {
                URL url = new URL(line);
                String host = url.getHost();
                if (host.startsWith("www.")) {
                    host = host.substring(4);
                }
                System.out.println("Adding " + host);
                STRINGS.add(host);
            } catch (MalformedURLException m) {
                System.out.println("MalformedURLException for " + line + " " + m);
            }
        }
    }
}
