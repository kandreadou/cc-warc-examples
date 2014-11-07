package org.commoncrawl.mklab.analysis;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.regex.Pattern;

/**
 * Created by kandreadou on 11/5/14.
 * NOTE: Not actively used. Testing.
 */
public class URLProcessor {

    private final static Pattern videoPattern = Pattern.compile("([^\\s]+(\\.(?i)(webm|mkv|flv|ogg|ogv|avi|mov|wmv|rm|mp4|m4v|mpg|mpeg|mp2|m2v|3gp|3g2|mxf))$)");
    private static Pattern youtubePattern = Pattern.compile("https*://www.youtube.com/watch?.*v=([a-zA-Z0-9_\\-]+)(&.+=.+)*");
    private static Pattern vimeoPattern = Pattern.compile("https*://vimeo.com/([0-9]+)/*$");
    private static Pattern dailymotionPattern = Pattern.compile("https*://www.dailymotion.com/video/([A-Za-z0-9]+)_.*$");

    private String imageUrl;

    private String pageUrl;

    private boolean success = false;

    public URLProcessor(String imageUrl, String pageUrl) {
        this.imageUrl = imageUrl.replaceAll("\\s", "");
        this.pageUrl = pageUrl.replaceAll("\\s", "");
    }

    public Result call(){
        normalize();
        process();
        return new Result(imageUrl, pageUrl, success);
    }


    protected void process() {
        //If the URL is unique
        boolean isUnique = false;
        synchronized (Statistics.UNIQUE_URLS) {
            //isUnique = Statistics.UNIQUE_URLS.put(imageUrl);
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
                imageUrl = url.toString();
                success = true;
                //System.out.println(url.toString());
            } catch (Exception e) {
                System.out.println("Failed to recontruct url " + url + "Exception " + e);
                success = false;
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
    }

}
