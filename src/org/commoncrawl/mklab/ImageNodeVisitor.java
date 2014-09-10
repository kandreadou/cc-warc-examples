package org.commoncrawl.mklab;

import com.google.common.base.Strings;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.parser.Tag;
import org.jsoup.select.NodeVisitor;

import java.io.IOException;
import java.text.DateFormat;


/**
 * Created by kandreadou on 9/9/14.
 */
public class ImageNodeVisitor implements NodeVisitor {

    private static final Logger LOG = Logger.getLogger(ImageNodeVisitor.class);

    private static final String[] VIDEO_SUFFIXES = {"3gp", "avi", "flv", "m4v", "mov", "mp4", "mpg", "mpeg", "swf", "wmv"};

    private static final String[] IMAGE_SUFFIXES = {"jpg", "jpeg", "png", "gif", "bmp"};

    private static final int TEXT_SIZE_LIMIT = 500;

    private static final Gson gson = new GsonBuilder()
            .setDateFormat(DateFormat.LONG)
            .setFieldNamingPolicy(FieldNamingPolicy.UPPER_CAMEL_CASE)
            .setVersion(1.0)
            .create();

    private Mapper.Context context;
    private String pageUrl;


    public ImageNodeVisitor(Mapper.Context context, String pageUrl) {
        this.context = context;
        this.pageUrl = pageUrl;
    }

    @Override
    public void head(Node node, int depth) {

    }

    @Override
    public void tail(Node node, int depth) {
        try {
            if (node instanceof Element) {
                Element e = (Element) node;
                // general stuff
                CCImage image = new CCImage();

                // specific for <img>
                if (e.tag() == Tag.valueOf("img")) {

                    image.src = e.attr("src");
                    image.alt = e.attr("alt");
                    image.height = e.attr("height");
                    image.width = e.attr("width");
                }
                // specific for <a> or <link>
                else if (e.tag() == Tag.valueOf("a") || e.tag() == Tag.valueOf("link")) {
                    String href = e.attr("href");
                    if (!Strings.isNullOrEmpty(href) && isImageUrl(href)) {
                        image.src = href;
                        image.alt = reduce(e.text());
                    }
                }
                // specific for <video>
                else if (e.tag() == Tag.valueOf("video")) {
                    image.height = e.attr("height");
                    image.width = e.attr("width");
                    image.src = e.attr("src");
                    image.alt = reduce(e.text());
                }
                // specific for <video> with multiple <source>
                else if (e.tag() == Tag.valueOf("source") && e.parent() != null && e.parent().tag() == Tag.valueOf("video")) {
                    Element p = e.parent();
                    image.height = p.attr("height");
                    image.width = p.attr("width");
                    image.alt = reduce(p.text());
                    image.src = e.attr("src");
                }
                // specific for <iframe> and <embed>
                else if (e.tag() == Tag.valueOf("iframe") || e.tag() == Tag.valueOf("embed")) {
                    image.height = e.attr("height");
                    image.width = e.attr("width");
                    image.alt = reduce(e.text());
                    if (!Strings.isNullOrEmpty(e.attr("src")) && isVideoUrl(e.attr("src")))
                        image.src = e.attr("src");
                }
                // specific for <object>
                else if (e.tag() == Tag.valueOf("object")){
                    image.height = e.attr("height");
                    image.width = e.attr("width");
                    image.alt = e.attr("name");
                    if (!Strings.isNullOrEmpty(e.attr("data")) && isVideoUrl(e.attr("data")))
                        image.src = e.attr("data");
                }
                if (!Strings.isNullOrEmpty(image.src)) {
                    image.pageUrl = pageUrl;
                    image.domSiblings = e.siblingElements().size();
                    image.domDepth = depth;
                    image.domElement = e.tag().getName();
                    Element parent = e.parent();
                    if (parent != null) {
                        image.parentTxt = reduce(parent.text());
                    }
                    context.write(new Text(image.src), new Text(gson.toJson(image) + ','));
                    //System.out.println(gson.toJson(image));
                }
            }
        } catch (InterruptedException ex) {
            LOG.debug(ex.getMessage(), ex);
        } catch (IOException ex) {
            LOG.debug(ex.getMessage(), ex);
        }
    }

    private static String reduce(String original) {
        if (!Strings.isNullOrEmpty(original)) {
            int limit = original.length() > TEXT_SIZE_LIMIT ? TEXT_SIZE_LIMIT : original.length();
            return original.substring(0, limit);
        }
        return null;
    }

    private static boolean isVideoUrl(String url) {
        return ArrayUtils.contains(VIDEO_SUFFIXES, url) || url.contains("youtube") || url.contains("vimeo");
    }

    private static boolean isImageUrl(String url){
        return ArrayUtils.contains(IMAGE_SUFFIXES, url);
    }

    public static void main(String[] args) throws Exception {

        String url = "http://www.awwwards.com/20-html5-video-websites-examples-and-resources.html";
        Document doc = Jsoup.connect(url).get();
        doc.traverse(new ImageNodeVisitor(null, url));

    }
}
