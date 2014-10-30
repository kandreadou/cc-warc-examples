package org.commoncrawl.mklab.analysis;

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
    private static int GLOBAL_COUNT = 0;
    private static int NEWS_COUNT = 0;
    private static int DOM_SIB = 0;
    private static int DOM_DEPTH = 0;
    private static Set<String> STRINGS = new HashSet<String>();
    DownloadService service = new DownloadService();

    public static void main(String[] args) throws Exception {

        CommonCrawlAnalyzer a = new CommonCrawlAnalyzer();
        a.readDomainsFromFile();
        a.analyzeCommonCrawlLocal();
    }

    protected void processLine(String jsonline) {
        try {
            JsonReader reader = new JsonReader(new StringReader(jsonline));
            reader.setLenient(true);
            B b = gson.fromJson(reader, B.class);
            if (b != null) {
                GLOBAL_COUNT++;
                DOM_SIB += b.domSib;
                DOM_DEPTH += b.domDepth;
                boolean lineConsumed = false;
                while (!lineConsumed) {
                    if (service.canAcceptMoreTasks()) {
                        service.submitTask(b.src);
                        lineConsumed = true;
                    }else{
                        service.printStatus();
                    }
                    DownloadService.Result r = service.tryGetResult();
                    while (r != null) {
                        //System.out.println("Image url: " + r.url);
                       // System.out.println("Content length: " + r.contentLength);
                        //System.out.println("Content type: " + r.contentType);
                        r = service.tryGetResult();
                    }
                }
                /*if (isURLRelevant(b.src)) {
                    NEWS_COUNT++;
                    System.out.println("New site url found: " + b.src + " " + b.pageUrl);
                    System.out.println("GLOBAL COUNT: " + GLOBAL_COUNT + " NEWS COUNT " + NEWS_COUNT);
                    System.out.println("Average dom siblings :" + DOM_SIB / GLOBAL_COUNT);
                    System.out.println("Average dom depth :" + DOM_DEPTH / GLOBAL_COUNT);
                }*/
            }
        } catch (JsonSyntaxException je) {
            JSON_SYNTAX_PROBLEM_COUNT++;
            System.out.println(je);
            System.out.println(jsonline);
            System.out.println("#NUM EXCEPTIONS "+JSON_SYNTAX_PROBLEM_COUNT);
        }
    }

    protected boolean isURLRelevant(String urlStr) {
        try {
            URL url = new URL(urlStr);
            String host = url.getHost();
            if (host.startsWith("www.")) {
                host = host.substring(4);
            }
            if (STRINGS.contains(host))
                return true;
            return false;
        } catch (MalformedURLException mue) {
            //This is not a valid URL. Return false.
            return false;
        }
    }

    protected void analyzeCommonCrawlLocal() throws IOException {
        File file = new File("/home/kandreadou/Documents/todo/");
        readRecursivelyInLocalFolder(file);
    }

    protected void readRecursivelyInLocalFolder(File folder) throws IOException {
        for (final File fileEntry : folder.listFiles()) {
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
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                processLine(line);
            }
        }
        System.out.println("GLOBAL COUNT: " + GLOBAL_COUNT + " NES COUNT " + NEWS_COUNT);
        System.out.println("Average dom siblings :" + DOM_SIB / GLOBAL_COUNT);
        System.out.println("Average dom depth :" + DOM_DEPTH / GLOBAL_COUNT);
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
                STRINGS.add(host);
            } catch (MalformedURLException m) {
                System.out.println("MalformedURLException for " + line + " " + m);
            }
        }
    }

    /**
     * {"src":"\n\t\t\t\t\t\t\t\thttp://s7ondemand1.scene7.com/is/image/MoosejawMB/10211640x1073912_zm?$thumb150$\n\t\t\t\t\t\t\t\t",
     * "alt":"ExOfficio Women\u0027s Crossback Diamond Dress",
     * "w":"",
     * "h":"",
     * "pageUrl":"http://www.moosejaw.com/moosejaw/shop/product_Isis-Women-s-Aida-Dress_10227940_10208_10000001_-1_",
     * "parentTxt":"ExOfficio Women\u0027s Crossback Diamond Dress",
     * "domSib":4,
     * "domDepth":23,
     * "domElem":"img"},
     *
     * @return
     */
    class B {
        public String src, alt, pageUrl, parentTxt, img;
        public int domSib, domDepth;

    }

    class C {
        public B[] items;
    }
}
