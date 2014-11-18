package org.commoncrawl.mklab.analysis;

import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.regex.Pattern;

/**
 * Created by kandreadou on 10/30/14.
 */
public class ProcessingService {

    private static final int NUM_DOWNLOAD_THREADS = 10;
    private static final int MAX_NUM_PENDING_TASKS = 10 * NUM_DOWNLOAD_THREADS;
    private static final int CONNECTION_TIMEOUT = 1000; // in millis
    private static final int READ_TIMEOUT = 1000; // in millis
    private final ExecutorService executor = Executors.newFixedThreadPool(NUM_DOWNLOAD_THREADS);
    private final CompletionService<Result> service = new ExecutorCompletionService<Result>(executor);
    private final ImageDAO dao = new ImageDAO();

    private final static Pattern videoPattern = Pattern.compile("([^\\s]+(\\.(?i)(webm|mkv|flv|ogg|ogv|avi|mov|wmv|rm|mp4|m4v|mpg|mpeg|mp2|m2v|3gp|3g2|mxf))$)");
    private static Pattern youtubePattern = Pattern.compile("https*://www.youtube.com/watch?.*v=([a-zA-Z0-9_\\-]+)(&.+=.+)*");
    private static Pattern vimeoPattern = Pattern.compile("https*://vimeo.com/([0-9]+)/*$");
    private static Pattern dailymotionPattern = Pattern.compile("https*://www.dailymotion.com/video/([A-Za-z0-9]+)_.*$");
    private final static String DOWNLOAD_FOLDER = "/media/kandreadou/New Volume/Pics/";
    private static final int MIN_CALL_INTERVAL = 250;

    private int numPendingTasks;
    private long lastDownLoadCall = 0;

    public void submitTask(CrawledImage image) {
        if (!StringUtils.isEmpty(image.src) && !StringUtils.isEmpty(image.pageUrl)) {
            Callable<Result> call = new Download(image);
            service.submit(call);
            numPendingTasks++;
        }
    }

    public Result tryGetResult() {
        try {

            Future<Result> future = service.poll();
            if (future != null) {
                numPendingTasks--;
                return future.get();
            }
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
        private String imageHost;
        private String pageUrl;
        private boolean success = false;
        private CrawledImage image;

        public Download(CrawledImage image) {
            this.imageUrl = image.src.replaceAll("\\s", "");
            this.pageUrl = image.pageUrl.replaceAll("\\s", "");
            this.image = image;
        }

        @Override
        public Result call() {

            normalize();
            image.normalizedSrc = imageUrl;
            process();
            return new Result(imageUrl, pageUrl, success);

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
            //printStatus();
        }

        protected void download(URL imgurl) {

            HttpURLConnection conn = null;
            FileOutputStream fos = null;
            ReadableByteChannel rbc = null;
            String imageFilename = DOWNLOAD_FOLDER + imgurl.hashCode();
            try {
                conn = (HttpURLConnection) imgurl.openConnection();
                conn.setInstanceFollowRedirects(true);
                conn.setConnectTimeout(CONNECTION_TIMEOUT);
                conn.setReadTimeout(READ_TIMEOUT);
                String ct = conn.getContentType();
                if (ct != null && ct.startsWith("image")) {
                    UUID id = UUID.nameUUIDFromBytes(imgurl.toString().getBytes());
                    image.id = id.toString();
                    String fileExtension = ct.substring(ct.indexOf('/')+1);
                    imageFilename = DOWNLOAD_FOLDER + id + "." + fileExtension;
                    rbc = Channels.newChannel(conn.getInputStream());
                    fos = new FileOutputStream(imageFilename);
                    fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
                    dao.save(image);
                    //System.out.println(id + "  " + imgurl + " " + conn.getContentType());
                }

            } catch (Exception e) {
                System.out.println("Exception at url: " + imgurl + e);
                File imageFile = new File(imageFilename);
                imageFile.delete();
            } finally {
                lastDownLoadCall = System.currentTimeMillis();
                try {
                    if (fos != null) {
                        fos.close();
                        fos = null;
                    }
                    if (rbc != null) {
                        rbc.close();
                        rbc = null;
                    }
                    if (conn != null) {
                        conn.disconnect();
                        conn = null;
                    }
                } catch (Exception ex) {
                    //ignore
                }
            }
        }

        protected boolean handleHostUrl(String url, boolean isWebPage) {
            try {
                boolean isUnique = false;
                URL page = new URL(url);
                String pageHost = page.getHost();
                if (!isWebPage) {
                    imageHost = pageHost;
                }
                synchronized (Statistics.UNIQUE_DOMAINS) {
                    isUnique = Statistics.UNIQUE_DOMAINS.put(pageHost);
                }
                if (isUnique)
                    Statistics.DOMAIN_COUNT++;
                if (pageHost.startsWith("www.")) {
                    pageHost = pageHost.substring(4);
                }
                if (CommonCrawlAnalyzer.STRINGS.contains(pageHost)) {
                    if (isWebPage) {
                        Statistics.NEWS_WEBPAGES_FREQUENCIES.add(pageHost);
                        Statistics.addImageUrlForHost(imageHost, pageHost);
                    } else if (isVideo(url)) {
                        Statistics.NEWS_VIDEO_FREQUENCIES.add(pageHost);
                    } else {
                        Statistics.NEWS_IMAGES_FREQUENCIES.add(pageHost);
                        if(CommonCrawlAnalyzer.CASES.contains(url)){
                            Statistics.CASES_FREQUENCIES.add(url);
                        }
                        long now = System.currentTimeMillis();
                        while (now - lastDownLoadCall < MIN_CALL_INTERVAL) {
                            Thread.sleep(MIN_CALL_INTERVAL);
                            now = System.currentTimeMillis();
                        }
                        download(page);
                    }
                }
                return true;
            } catch (MalformedURLException mue) {
                //System.out.println("handleHostUrl(): " + mue);
                return false;
            } catch (InterruptedException ie) {
                return false;
            }
        }

        protected void normalize() {

            URL url = null;
            try {
                url = new URL(imageUrl);
            } catch (Exception mue) {
                if (mue.getMessage().startsWith("no protocol")) {
                    try {
                        url = new URL("http://" + imageUrl);
                        imageUrl = url.toString();
                        success = true;
                    } catch (Exception e) {
                        success = false;
                    }
                } else
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
                        imageUrl = url.toString();
                        success = true;
                        //System.out.println(url.toString());
                    } catch (Exception e) {
                        //System.out.println("Failed to recontruct url " + url + "Exception " + e);
                        success = false;
                    }
            }
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
        ProcessingService service = new ProcessingService();
        if (service.canAcceptMoreTasks()) {
            //service.submitTask("c.o0bg.com/rf/image_r/Boston/2011-2020/2014/02/05/BostonGlobe.com/EditorialOpinion/Images/0205toon_wasserman-1311.jpg", "www.google.com", null);
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
        //String url = "http://www.efsyn.gr/";
        //System.out.println(isVideo(url));
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
