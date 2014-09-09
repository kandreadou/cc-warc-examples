package org.commoncrawl.mklab;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
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
                if (e.tag() == Tag.valueOf("img")) {
                    String src = e.attr("src");
                    if (src != null && !StringUtils.isEmpty(src)) {
                        CCImage image = new CCImage();
                        image.src = src;
                        image.alt = e.attr("alt");
                        image.height = e.attr("height");
                        image.width = e.attr("width");
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
                        context.write(new Text(src), new Text(gson.toJson(image) + ','));
                    }
                } else if (e.tag() == Tag.valueOf("a")) {
                    String href = e.attr("href");
                    if (href != null && !StringUtils.isEmpty(href) && (href.endsWith("jpg") || href.endsWith("png") || href.endsWith("gif"))) {
                        CCImage image = new CCImage();
                        image.src = href;
                        image.alt = e.text();
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
                        context.write(new Text(href), new Text(gson.toJson(image) + ','));
                    }
                }
            }
        } catch (InterruptedException ex) {
            LOG.debug(ex.getMessage(), ex);
        } catch (IOException ex) {
            LOG.debug(ex.getMessage(), ex);
        }
    }
}
