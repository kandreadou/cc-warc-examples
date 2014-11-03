package org.commoncrawl.mklab.analysis;

import com.google.common.hash.BloomFilter;
import org.apache.commons.lang.StringUtils;

import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by kandreadou on 10/30/14.
 */
public class DownloadService {

    private static final int NUM_DOWNLOAD_THREADS = 1000;
    private static final int MAX_NUM_PENDING_TASKS = 10 * NUM_DOWNLOAD_THREADS;
    private static final int CONNECTION_TIMEOUT = 2000; // in millis
    private static final int READ_TIMEOUT = 2000; // in millis
    private final ExecutorService executor = Executors.newFixedThreadPool(NUM_DOWNLOAD_THREADS);
    private final CompletionService<Result> service = new ExecutorCompletionService<Result>(executor);

    private final static Pattern videoPattern = Pattern.compile("([^\\s]+(\\.(?i)(webm|mkv|flv|ogg|ogv|avi|mov|wmv|rm|mp4|m4v|mpg|mpeg|mp2|m2v|3gp|3g2|mxf))$)");
    private static Pattern youtubePattern = Pattern.compile("https*://www.youtube.com/watch?.*v=([a-zA-Z0-9_\\-]+)(&.+=.+)*");
    private static Pattern vimeoPattern = Pattern.compile("https*://vimeo.com/([0-9]+)/*$");
    private static Pattern dailymotionPattern = Pattern.compile("https*://www.dailymotion.com/video/([A-Za-z0-9]+)_.*$");


    private int numPendingTasks;

    public void submitTask(String url, String pageUrl) {
        if (!StringUtils.isEmpty(url) && !StringUtils.isEmpty(pageUrl)) {
            Callable<Result> call = new Download(url, pageUrl);
            service.submit(call);
            numPendingTasks++;
        }
    }

    public Result tryGetResult() {
        try {
            numPendingTasks--;
            Future<Result> future = service.poll();
            if (future != null)
                return future.get();
            return null;
        } catch (Exception e) {
            System.out.println("Exception in getResultWait: " + e);
            return null;
        }
    }

    public boolean canAcceptMoreTasks() {
        return numPendingTasks < MAX_NUM_PENDING_TASKS;
    }

    public void printStatus() {
        System.out.println("Pending tasks: " + numPendingTasks);
    }

    public void shutDown() {
        executor.shutdown(); // Disable new tasks from being submitted
        try {
            // Wait a while for existing tasks to terminate
            if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                executor.shutdownNow(); // Cancel currently executing tasks
                // Wait a while for tasks to respond to being cancelled
                if (!executor.awaitTermination(60, TimeUnit.SECONDS))
                    System.err.println("Pool did not terminate");
            }
        } catch (InterruptedException ie) {
            // (Re-)Cancel if current thread also interrupted
            executor.shutdownNow();
            // Preserve interrupt status
            Thread.currentThread().interrupt();
        }
    }

    class Download implements Callable<Result> {

        private String imageUrl;
        private String pageUrl;
        private boolean success = false;

        public Download(String imageUrl, String pageUrl) {
            this.imageUrl = imageUrl.replaceAll("\\s", "");
            this.pageUrl = pageUrl.replaceAll("\\s", "");
            //System.out.println("imageUrl "+imageUrl+" pageUrl "+pageUrl);
        }

        @Override
        public Result call() {

            normalize();
            process();

            return new Result(imageUrl, pageUrl, success);

                /*
                try {
                    conn = (HttpURLConnection) url.openConnection();
                    conn.setInstanceFollowRedirects(true);
                    conn.setConnectTimeout(CONNECTION_TIMEOUT); // TO DO: add retries when connections times out
                    conn.setReadTimeout(READ_TIMEOUT);
                    //conn.connect();
                    //System.out.println("Content length: " + conn.getContentLength() + " Content type: " + conn.getContentType());
                    success = true;
                } catch (Exception e) {
                    System.out.println("Exception at url: " + url);
                } finally {
                    Result result;
                    if (conn != null) {
                        result = new Result(url.toString(), success, conn.getContentLength(), conn.getContentType());
                        conn.disconnect();
                    } else {
                        result = new Result(imageUrl, false);
                    }
                    return result;
                }
                */
        }

        protected void process() {
            //If the URL is unique
            boolean isUnique = false;
            synchronized (Statistics.UNIQUE_URLS) {
                isUnique = Statistics.UNIQUE_URLS.put(imageUrl);
            }
            if (isUnique) {
                Statistics.GLOBAL_COUNT++;
                //Handle imageUrl
                success &= handleHostUrl(imageUrl, false);
                success &= handleHostUrl(pageUrl, true);
            }
        }

        protected boolean handleHostUrl(String url, boolean isWebPage) {
            try {
                boolean isUnique = false;
                URL page = new URL(url);
                String pageHost = page.getHost();
                synchronized (Statistics.UNIQUE_DOMAINS) {
                    isUnique = Statistics.UNIQUE_DOMAINS.put(pageHost);
                }
                if (isUnique)
                    Statistics.DOMAIN_COUNT++;
                if (pageHost.startsWith("www.")) {
                    pageHost = pageHost.substring(4);
                }
                if (CommonCrawlAnalyzer.STRINGS.contains(pageHost)) {
                    if (isWebPage)
                        Statistics.NEWS_WEBPAGES_FREQUENCIES.add(pageHost);
                    else if (isVideo(url))
                        Statistics.NEWS_VIDEO_FREQUENCIES.add(pageHost);
                    else
                        Statistics.NEWS_IMAGES_FREQUENCIES.add(pageHost);
                }
                return true;
            } catch (MalformedURLException mue) {
                System.out.println("handleHostUrl(): " + mue);
                return false;
            }
        }

        protected void normalize() {
            URL url = null;
            try {
                url = new URL(imageUrl);
            } catch (Exception ex) {
                //System.out.println("Attempting to recontruct url from pageUrl "+pageUrl+" imageUrl "+imageUrl);
                try {
                    URL baseUrl = new URL(pageUrl);
                    // check if the imageUrl path is like this ../..
                    String[] elements = imageUrl.split("\\.\\.");
                    int len = elements.length;
                    if (len > 1) {
                        //System.out.println("^^^"+ Arrays.toString(elements));
                        imageUrl = elements[len - 1];
                        //System.out.println("##### "+imageUrl);
                        String baseUrlPath = baseUrl.getPath();
                        int lastIndexOfSlash = baseUrlPath.lastIndexOf('/');
                        while (len > 0 && lastIndexOfSlash > 2) {
                            baseUrlPath = baseUrlPath.substring(0, lastIndexOfSlash);
                            //System.out.println("########page "+pageUrl);
                            len--;
                            lastIndexOfSlash = baseUrlPath.lastIndexOf('/');
                        }
                        url = new URL(baseUrl.getProtocol() + "://" + baseUrl.getHost() + baseUrlPath + imageUrl);
                    } else {
                        //remove slash at the end of the host name
                        String host = baseUrl.getHost().substring(0, baseUrl.getHost().length());
                        url = new URL(baseUrl.getProtocol() + "://" + host + imageUrl);
                    }
                    //System.out.println(url.toString());
                } catch (Exception e) {
                    System.out.println("Failed to recontruct url " + url + "Exception " + e);
                    success = false;
                }
            }
            imageUrl = url.toString();
            success = true;
        }
    }

    public static boolean isVideo(String url) {
        return videoPattern.matcher(url).matches() || youtubePattern.matcher(url).matches() || vimeoPattern.matcher(url).matches() || dailymotionPattern.matcher(url).matches();
    }


    public class Result {
        public String url;
        public String pageUrl;
        public boolean success = false;
        public int contentLength;
        public String contentType;

        public Result(String url, String pageUrl, boolean success) {
            this.url = url;
            this.pageUrl = pageUrl;
            this.success = success;
        }

        public Result(String url, boolean success, int contentLength, String contentType) {
            this.url = url;
            this.success = success;
            this.contentLength = contentLength;
            this.contentType = contentType;
        }

    }

    public static void main(String[] args) {
        String url = "http://www.efsyn.gr/";
        System.out.println(isVideo(url));
        /*String imageUrl = "/images/content/pagebuilder/KAD12-Logo-Header2.png";
        String pageUrl = "http://www2.kidneyfund.org/site/TR/WalkKADEvent/KidneyActionDay?pg=objcon&fr_id=1360&px=1504700";
        HttpURLConnection conn = null;
        boolean success = false;
        URL url = null;
        try {
            url = new URL(imageUrl);
        } catch (Exception ex) {
            //System.out.println("Attempting to recontruct url from pageUrl "+pageUrl+" imageUrl "+imageUrl);
            try {
                URL baseUrl = new URL(pageUrl);
                // check if the imageUrl path is like this ../..
                String[] elements = imageUrl.split("\\.\\.");
                int len = elements.length;
                if (len > 1) {
                    //System.out.println("^^^"+ Arrays.toString(elements));
                    imageUrl = elements[len - 1];
                    //System.out.println("##### "+imageUrl);
                    String baseUrlPath = baseUrl.getPath();
                    int lastIndexOfSlash = baseUrlPath.lastIndexOf('/');
                    while (len > 0 && lastIndexOfSlash > 2) {
                        baseUrlPath = baseUrlPath.substring(0, lastIndexOfSlash);
                        //System.out.println("########page "+pageUrl);
                        len--;
                        lastIndexOfSlash = baseUrlPath.lastIndexOf('/');
                    }
                    url = new URL(baseUrl.getProtocol() + "://" + baseUrl.getHost() + baseUrlPath + imageUrl);
                    System.out.println("########reconstructed " + url);
                    System.out.println("########from " + pageUrl + "  " + imageUrl);
                } else {
                    //remove slash at the end of the host name
                    String host = baseUrl.getHost().substring(0, baseUrl.getHost().length());
                    url = new URL(baseUrl.getProtocol() + "://" + host + imageUrl);
                }
                //System.out.println(url.toString());
            } catch (Exception e) {
                System.out.println("Failed to recontruct url " + url + "Exception " + e);

            }
        }
        try {
            conn = (HttpURLConnection) url.openConnection();
            conn.setInstanceFollowRedirects(true);
            conn.setConnectTimeout(CONNECTION_TIMEOUT); // TO DO: add retries when connections times out
            conn.setReadTimeout(READ_TIMEOUT);
            //conn.connect();
            //System.out.println("Content length: " + conn.getContentLength() + " Content type: " + conn.getContentType());
            success = true;
        } catch (Exception e) {
            System.out.println("Exception at url: " + url);
        } finally {
            Result result;
            if (conn != null) {

                conn.disconnect();
            } else {
            }
        }*/
    }
}
