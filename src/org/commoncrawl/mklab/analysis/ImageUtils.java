package org.commoncrawl.mklab.analysis;

import gr.iti.mklab.simmo.items.Image;
import gr.iti.mklab.simmo.morphia.MediaDAO;
import gr.iti.mklab.visual.utilities.ImageIOGreyScale;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.http.HttpStatus;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Date;

/**
 * Created by kandreadou on 1/13/15.
 */
public class ImageUtils {

    private final static int MIN_CONTENT_LENGTH = 20000;
    private final static int MIN_WIDTH = 400;
    private final static int MIN_HEIGHT = 400;
    /**
     * The value to use in HttpURLConnection.setConnectTimeout()
     */
    public static final int connectionTimeout = 3000;
    /**
     * The value to use in HttpURLConnection.setReadTimeout()
     */
    public static final int readTimeout = 2000;

    public static long lastLastModified;

    public static void main(String[] args) throws Exception {
        gr.iti.mklab.simmo.morphia.MorphiaManager.setup("160.40.51.20");
        MediaDAO<Image> imageDAO = new MediaDAO<Image>(Image.class, "review29jan15");
        String imgurl = "https://thechive.files.wordpress.com/2014/08/famous-pranks-hoaxes-fakes-42.jpg";
        HttpClient client = new HttpClient();
        HttpMethod method = null;

        try {
            BufferedImage input = ImageUtils.downloadImage(imgurl.toString());
            //BufferedImage input = ImageIO.read(new File("/home/kandreadou/Desktop/theorectangle.png"));

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
                    img.setAlternateText("jane fonda");
                    //img.setDescription("gawker");
                    //img.setWebPageUrl("http://left.gr");
                    img.setUrl(imgurl);
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
        gr.iti.mklab.simmo.morphia.MorphiaManager.tearDown();
    }

    public static boolean checkContentHeaders(int contentLength, String contentType) {
        return contentLength > MIN_CONTENT_LENGTH && contentType.startsWith("image");
    }

    public static boolean checkImage(BufferedImage img) {
        return img != null && img.getWidth() >= MIN_WIDTH && img.getHeight() >= MIN_HEIGHT;
    }

    public static BufferedImage downloadImage(String imageUrl) throws Exception {
        BufferedImage image = null;
        InputStream in = null;
        try { // first try reading with the default class
            URL url = new URL(imageUrl);
            HttpURLConnection conn = null;
            boolean success = false;
            try {
                conn = (HttpURLConnection) url.openConnection();
                conn.setInstanceFollowRedirects(true);
                conn.setConnectTimeout(connectionTimeout); // TO DO: add retries when connections times out
                conn.setReadTimeout(readTimeout);
                conn.connect();
                lastLastModified = conn.getLastModified();
                success = true;
            } catch (Exception e) {
                System.out.println("Connection related exception at url: " + imageUrl);
            } finally {
                if (!success) {
                    conn.disconnect();
                }
            }
            success = false;
            try {
                in = conn.getInputStream();
                success = true;
            } catch (Exception e) {
                System.out.println("Exception when getting the input stream from the connection at url: "
                        + imageUrl);
            } finally {
                if (!success) {
                    in.close();
                }
            }
            image = ImageIO.read(in);
        } catch (IllegalArgumentException e) {
// this exception is probably thrown because of a greyscale jpeg image
            System.out.println("Exception: " + e.getMessage() + " | Image: " + imageUrl);
            image = ImageIOGreyScale.read(in); // retry with the modified class
        } catch (MalformedURLException e) {
            System.out.println("Malformed url exception. Url: " + imageUrl);
        }
        return image;
    }
}
