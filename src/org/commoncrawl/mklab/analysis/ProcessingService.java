package org.commoncrawl.mklab.analysis;

import gr.iti.mklab.simmo.items.Image;
import gr.iti.mklab.simmo.morphia.MediaDAO;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.util.DateUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.protocol.HTTP;
import org.bson.types.ObjectId;
import org.commoncrawl.mklab.ngrams.FeaturesArffCreator;
import org.commoncrawl.mklab.ngrams.ScoreNgramArffCreator;
import weka.classifiers.trees.RandomForest;
import weka.core.*;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.regex.Pattern;

/**
 * Created by kandreadou on 10/30/14.
 */
public class ProcessingService {

    private static final int NUM_DOWNLOAD_THREADS = 100;
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
    private final static String DOWNLOAD_FOLDER = "/media/kandreadou/New Volume/Pics_benchmark/";
    private static final int MIN_CALL_INTERVAL = 150;

    private int numPendingTasks;
    private long lastDownLoadCall = 0;
    private HttpClient client;
    private MediaDAO<Image> imageDAO;

    private MessageDigest md;

    private RandomForest featuresClass;
    private RandomForest ngramsClass;
    private ScoreNgramArffCreator ngramsExtractor;
    private Instances featureData;
    private Instances ngramData;

    public ProcessingService() {
        //IndexingManage.getInstance();
        gr.iti.mklab.simmo.morphia.MorphiaManager.setup("127.0.0.1");
        imageDAO = new MediaDAO<Image>(Image.class, "demopaper20K");
        client = new HttpClient(new MultiThreadedHttpConnectionManager());

        //establish a connection within 5 seconds
        client.getHttpConnectionManager().
                getParams().setConnectionTimeout(CONNECTION_TIMEOUT);

        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException nae) {
            System.out.println(nae);
        }

        try {
            FileInputStream fis = new FileInputStream("/home/kandreadou/Documents/commoncrawlstuff/models/tfidf_2000_100trees.model");
            ObjectInputStream ois = new ObjectInputStream(fis);
            ngramsClass = (RandomForest) ois.readObject();
            ois.close();
            fis = new FileInputStream("/home/kandreadou/Documents/commoncrawlstuff/models/features_30trees.model");
            ois = new ObjectInputStream(fis);
            featuresClass = (RandomForest) ois.readObject();
            ois.close();
            ngramsExtractor = new ScoreNgramArffCreator("/home/kandreadou/Documents/commoncrawlstuff/junk.txt", 2000, "/home/kandreadou/Documents/commoncrawlstuff/ngrams_tfidf_2000.txt");
            ngramsExtractor.readNgramsFromFile();
            featureData = createFeatureData();
            ngramData = createNGramData();
        } catch (Exception cnf) {
            System.out.println(cnf);
        }


    }

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
        gr.iti.mklab.simmo.morphia.MorphiaManager.tearDown();
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

    private static int index =0;
    private static long duration = 0;

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

        protected synchronized void process() {
            //If the URL is unique
            boolean isUnique = false;
            synchronized (Statistics.UNIQUE_URLS) {
                isUnique = Statistics.UNIQUE_URLS.put(imageUrl);
            }
            if (isUnique) {
                Statistics.GLOBAL_COUNT++;
                try {
                    URL page = new URL(imageUrl);
                    long start = System.currentTimeMillis();
                    double[] featureVector = getFeatureVector(image);
                    featureData.add(new DenseInstance(1, featureVector));
                    double[] ngramVector = getngramvec(image);
                    ngramData.add(new SparseInstance(1, ngramVector));

                    Instance n = ngramData.get(index);
                    Instance f = featureData.get(index);

                    double c1 = ngramsClass.classifyInstance(n);
                    //isBig = c1==1;
                    double c2 = featuresClass.classifyInstance(f);
                    //The classifiers agree
                    boolean isBig = false;
                    if (c1 == c2) {
                        isBig = c1 == 1;
                        //System.out.println("The classifiers agree " + c1);
                    } else {

                        double[] d1 = ngramsClass.distributionForInstance(n);
                        //System.out.println("d1 " + d1[0] + " " + d1[1]);
                        double[] d2 = featuresClass.distributionForInstance(f);
                        //System.out.println("d2 " + d2[0] + " " + d2[1]);
                        if (Math.abs(d1[0] - d1[1]) + 0.05 > Math.abs(d2[0] - d2[1]))
                            isBig = c1 == 1;
                        else
                            isBig = c2 == 1;
                    }
                    index++;
                    long d = System.currentTimeMillis() - start;
                    duration+=d;
                    long averageDuration = duration/index;
                    System.out.println("duration "+d);
                    System.out.println("average duration "+averageDuration);
                    if (!isBig)
                        return;

                    if (page != null) {
                        String pageHost = page.getHost();
                        imageHost = pageHost;
                        long now = System.currentTimeMillis();
                        while (now - lastDownLoadCall < MIN_CALL_INTERVAL) {
                            Thread.sleep(MIN_CALL_INTERVAL);
                            now = System.currentTimeMillis();
                        }
                        download(page);
                    }
                } catch (MalformedURLException mue) {
                    //ignore
                } catch (InterruptedException ie) {
                    //ignore
                } catch (Exception e) {

                }

            }

        }

        protected void processOld() {
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

        // new for the demo paper
        protected synchronized void newStoreAndIndex(URL imgurl) {
            try {
                BufferedImage input = ImageUtils.downloadImage(imgurl.toString());
                if (input == null || input.getWidth() < 400 || input.getHeight() < 400)
                    return;
                if (IndexingManage.getInstance().indexImage(imgurl.toString(), input)) {
                    Image img = new Image();
                    img.setId(new ObjectId().toString());
                    img.setHeight(input.getWidth());
                    img.setWidth(input.getWidth());
                    img.setAlternateText(image.alt);
                    img.setDescription(image.parentTxt);
                    img.setWebPageUrl(image.pageUrl);
                    img.setUrl(image.normalizedSrc);
                    //img.setLastModifiedDate(new Date(lastModified));
                    imageDAO.save(img);
                }
            } catch (Exception e) {
                //System.out.println(e);
            }
        }

        //new for the review meeting
        protected synchronized void storeAndIndex(URL imgurl) {

            //System.out.println("Store and index "+imgurl);
            HttpMethod method = null;

            try {
                BufferedImage input = ImageUtils.downloadImage(imgurl.toString());
                long lastModified = ImageUtils.lastLastModified;
                if (ImageUtils.checkImage(input)) {
                    System.out.println("Getting " + imgurl);
                    method = new GetMethod("http://160.40.51.20:8080/reveal/mmapi/media/review29jan15/index?imageurl=" + URLEncoder.encode(imgurl.toString(), "UTF-8"));
                    method.setFollowRedirects(true);
                    int statusCode = client.executeMethod(method);

                    if (statusCode != HttpStatus.SC_OK) {
                        System.out.println("HTTP not ok for " + imgurl.toString());
                    } else {
                        Image img = new Image();
                        img.setHeight(input.getWidth());
                        img.setWidth(input.getWidth());
                        img.setAlternateText(image.alt);
                        img.setDescription(image.parentTxt);
                        img.setWebPageUrl(image.pageUrl);
                        img.setUrl(image.normalizedSrc);
                        img.setLastModifiedDate(new Date(lastModified));
                        imageDAO.save(img);
                    }
                }
            } catch (Exception e) {
                System.out.println(e);
            } finally {
                if (method != null)
                    method.releaseConnection();
            }
        }

        protected void download(URL imgurl) {

            //System.out.println(imgurl.toString());

            //GetMethod method = new GetMethod(imgurl.toString());
            //method.setFollowRedirects(true);
            //int statusCode = client.executeMethod(method);
            //method.getResponseBodyAsStream()


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
                    if (!dao.exists("_id", id.toString())) {
                        String fileExtension = ct.substring(ct.indexOf('/') + 1);
                        imageFilename = DOWNLOAD_FOLDER + id + "." + fileExtension;
                        image.filename = image.id + '.' + fileExtension;
                        rbc = Channels.newChannel(conn.getInputStream());
                        fos = new FileOutputStream(imageFilename);
                        fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
                        dao.save(image);
                    }
                    //System.out.println(id + "  " + imgurl + " " + conn.getContentType());
                }

            } catch (Exception e) {
                System.out.println("Exception at url: " + imgurl + e);
                File imageFile = new File(imageFilename);
                if (imageFile.exists())
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
                    System.out.println("Connection exception " + ex);
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
                //if (CommonCrawlAnalyzer.STRINGS.contains(pageHost)) {
                if (isWebPage) {
                    Statistics.NEWS_WEBPAGES_FREQUENCIES.add(pageHost);
                    Statistics.addImageUrlForHost(imageHost, pageHost);
                } else if (isVideo(url)) {
                    Statistics.NEWS_VIDEO_FREQUENCIES.add(pageHost);
                } else {
                    Statistics.NEWS_IMAGES_FREQUENCIES.add(pageHost);
                    if (CommonCrawlAnalyzer.CASES.contains(url)) {
                        Statistics.CASES_FREQUENCIES.add(url);
                    }
                    long now = System.currentTimeMillis();
                    while (now - lastDownLoadCall < MIN_CALL_INTERVAL) {
                        Thread.sleep(MIN_CALL_INTERVAL);
                        now = System.currentTimeMillis();
                    }
                    download(page);
                    //newStoreAndIndex(page);
                }
                //}
                return true;
            } catch (MalformedURLException mue) {
                //System.out.println("handleHostUrl(): " + mue);
                return false;
            } catch (InterruptedException ie) {
                System.out.println("InterruptedException " + ie);
                return false;
            }
        }

        protected void normalize() {

            URL url = null;
            try {
                url = new URL(imageUrl);
            } catch (Exception mue) {

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
                        url = new URL(baseUrl.getProtocol() + "://" + host + (imageUrl.startsWith("/") ? "" : "/") + imageUrl);
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
        MorphiaManager.setup("127.0.0.1");
        ProcessingService service = new ProcessingService();
        CrawledImage i = new CrawledImage();
        i.src = "typo3temp/pics/4eaab97c99.jpg";
        i.pageUrl = "http://www.unradio.unal.edu.co/detalle/cy/2012/cm/4/article/autismo-ii.html";
        if (service.canAcceptMoreTasks()) {

            service.submitTask(i);
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
            System.out.println(i.normalizedSrc);
        }
        MorphiaManager.tearDown();
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

    private double[] getFeatureVector(CrawledImage i) {

        int index = 0;
        double[] featureVector = new double[23];
        String imUrl = i.normalizedSrc;
        //System.out.println(i.normalizedSrc);
        String imName = FeaturesArffCreator.getImageName(i.normalizedSrc);
        String suffix = FeaturesArffCreator.getSuffix(imName);
        //System.out.println(suffix);

        featureVector[index] = "jpeg".equals(suffix) ? 1 : 0;
        index++;
        featureVector[index] = "png".equals(suffix) ? 1 : 0;
        index++;
        featureVector[index] = "bmp".equals(suffix) ? 1 : 0;
        index++;
        featureVector[index] = "gif".equals(suffix) ? 1 : 0;
        index++;
        featureVector[index] = "tiff".equals(suffix) ? 1 : 0;
        index++;
        featureVector[index] = i.domDepth;
        index++;
        featureVector[index] = i.domSib;
        index++;


        //estimated dimensions from url
        int[] dims = new int[2];
        try {
            dims = FeaturesArffCreator.extractNumeric(i.normalizedSrc);
        }catch(NumberFormatException e){
            System.out.println("NumberFormatException caught");
        }
        featureVector[index] = dims[0] > 0 ? 1 : 0;
        index++;
        featureVector[index] = dims[0];
        index++;
        featureVector[index] = dims[1] > 0 ? 1 : 0;
        index++;
        featureVector[index] = dims[1];
        index++;

        try {
            URL page = new URL(imUrl);
            String imHost = page.getHost();
            page = new URL(i.pageUrl);
            String pageHost = page.getHost();
            featureVector[index] = imHost.equalsIgnoreCase(pageHost) ? 1 : 0;
            index++;
        } catch (MalformedURLException ex) {
            System.out.println(ex);
            featureVector[index] = 0;
            index++;
        }

        featureVector[index] = "img".equals(i.domElem) ? 1 : 0;
        index++;
        featureVector[index] = "link".equals(i.domElem) ? 1 : 0;
        index++;
        featureVector[index] = "a".equals(i.domElem) ? 1 : 0;
        index++;
        featureVector[index] = "embed".equals(i.domElem) ? 1 : 0;
        index++;
        featureVector[index] = "iframe".equals(i.domElem) ? 1 : 0;
        index++;
        featureVector[index] = "object".equals(i.domElem) ? 1 : 0;
        index++;


        if (StringUtils.isEmpty(i.alt)) {
            featureVector[index] = 0;
            index++;
            featureVector[index] = 0;
            index++;
        } else {
            featureVector[index] = 1;
            index++;
            featureVector[index] = i.alt.length();
            index++;
        }
        if (StringUtils.isEmpty(i.parentTxt)) {
            featureVector[index] = 0;
            index++;
            featureVector[index] = 0;
            index++;
        } else {
            featureVector[index] = 1;
            index++;
            featureVector[index] = i.parentTxt.length();
            index++;
        }

        featureVector[index] = i.normalizedSrc.length();
        return featureVector;
    }

    private Instances createFeatureData() {
        ArrayList<Attribute> attributes = new ArrayList<Attribute>();
        attributes.add(new Attribute("suffix_JEPG"));
        attributes.add(new Attribute("suffix_PNG"));
        attributes.add(new Attribute("suffix_BMP"));
        attributes.add(new Attribute("suffix_GIF"));
        attributes.add(new Attribute("suffix_TIFF"));
        attributes.add(new Attribute("domDepth"));
        attributes.add(new Attribute("domSiblings"));
        attributes.add(new Attribute("hasWidth"));
        attributes.add(new Attribute("width"));
        attributes.add(new Attribute("hasHeight"));
        attributes.add(new Attribute("height"));
        attributes.add(new Attribute("samedomain"));
        attributes.add(new Attribute("domElement_IMG"));
        attributes.add(new Attribute("domElement_LINK"));
        attributes.add(new Attribute("domElement_A"));
        attributes.add(new Attribute("domElement_EMBED"));
        attributes.add(new Attribute("domElement_IFRAME"));
        attributes.add(new Attribute("domElement_OBJECT"));
        attributes.add(new Attribute("hasAltText"));
        attributes.add(new Attribute("altTextLength"));
        attributes.add(new Attribute("hasParentText"));
        attributes.add(new Attribute("parentTextLength"));
        attributes.add(new Attribute("urlLength"));
        ArrayList fvClassVal = new ArrayList<String>(2);
        fvClassVal.add("SMALL");
        fvClassVal.add("BIG");
        Attribute classAttribute = new Attribute("class", fvClassVal);
        attributes.add(classAttribute);
        Instances data = new Instances("New instance", attributes, 10*1000*1000);
        data.setClassIndex(23);
        return data;
    }

    private double[] getngramvec(CrawledImage i) {
        boolean[] ngramVector = ngramsExtractor.getNGramVector(i);
        double[] sparseVector = new double[ngramVector.length];
        for (int k = 0; k < ngramVector.length; k++)
            sparseVector[k] = ngramVector[k] ? 1 : 0;
        return sparseVector;
    }

    private Instances createNGramData() throws Exception {
        ArrayList<Attribute> attributes = new ArrayList<Attribute>();
        for (int i = 0; i < 2000; i++) {
            attributes.add(new Attribute("ngram" + i));
        }
        ArrayList fvClassVal = new ArrayList<String>(2);
        fvClassVal.add("SMALL");
        fvClassVal.add("BIG");
        Attribute classAttribute = new Attribute("class", fvClassVal);
        attributes.add(classAttribute);
        // predict instance class values
        Instances data = new Instances("New instance", attributes, 10*1000*1000);
        data.setClassIndex(2000);
        return data;
    }
}
