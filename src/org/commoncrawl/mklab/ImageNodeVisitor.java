package org.commoncrawl.mklab;

import com.google.common.base.Strings;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.parser.Tag;
import org.jsoup.select.Elements;
import org.jsoup.select.NodeVisitor;

import java.io.IOException;
import java.text.DateFormat;


/**
 * Created by kandreadou on 9/9/14.
 */
public class ImageNodeVisitor implements NodeVisitor {

    private static final Logger LOG = Logger.getLogger(ImageNodeVisitor.class);


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
                    if (href != null && !StringUtils.isEmpty(href) && (href.endsWith("jpg") || href.endsWith("png") || href.endsWith("gif"))) {
                        image.src = href;
                        image.alt = e.text();
                    }
                }
                // specific for <video>
                else if (e.tag() == Tag.valueOf("video")) {
                    image.height = e.attr("height");
                    image.width = e.attr("width");
                    image.src = e.attr("src");
                    image.alt = "####VIDEO### " + e.text();
                }
                // specific for <video> with multiple <source>
                else if (e.tag() == Tag.valueOf("source") && e.parent() != null && e.parent().tag() == Tag.valueOf("video")) {
                    Element p = e.parent();
                    image.height = p.attr("height");
                    image.width = p.attr("width");
                    image.alt = "####VIDEO### " + p.text();
                    image.src = e.attr("src");
                }
                if (!Strings.isNullOrEmpty(image.src)) {
                    image.pageUrl = pageUrl;
                    image.domSiblings = e.siblingElements().size();
                    image.domDepth = depth;
                    Element parent = e.parent();
                    if (parent != null) {
                        String parentText = parent.text();
                        if (parentText != null && !StringUtils.isEmpty(parentText)) {
                            int limit = parentText.length() > 500 ? 500 : parentText.length();
                            image.parentTxt = parent.text().substring(0, limit);
                        }
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

    public static void main(String[] args) throws Exception {

        String url = "http://www.awwwards.com/20-html5-video-websites-examples-and-resources.html";
        Document doc = Jsoup.connect(url).get();
        doc.traverse(new ImageNodeVisitor(null, url));

    }
}
