package org.commoncrawl.mklab;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
import org.archive.io.warc.WARCReaderFactory;
import org.json.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by kandreadou on 9/5/14.
 */
public class ImageMap {

    private static final Logger LOG = Logger.getLogger(ImageMap.class);

    protected static enum MAPPERCOUNTER {
        RECORDS_IN,
        EXCEPTIONS
    }

    public static class ImageMapper extends Mapper<Text, ArchiveReader, Text, Text> {

        @Override
        public void map(Text key, ArchiveReader value, Context context) throws IOException {

            for (ArchiveRecord r : value)
                try {
                    LOG.debug(r.getHeader().getUrl() + " -- " + r.available());
                    // We're only interested in processing the responses, not requests or metadata
                    if (r.getHeader().getMimetype().equals("application/http; msgtype=response")) {
                        // Convenience function that reads the full message into a raw byte array
                        byte[] rawData = IOUtils.toByteArray(r, r.available());
                        String content = new String(rawData);
                        String body = content.substring(content.indexOf("\r\n\r\n") + 4);
                        Document doc = Jsoup.parse(content);

                        Elements mf = doc.getElementsByTag("img");

                        if (mf.size() > 0) {
                            for (Element e : mf) {
                                String src = e.attr("src");

                                if (src != null && !StringUtils.isEmpty(src)) {
                                    CCImage image = new CCImage();
                                    image.src = src;
                                    image.alt = e.attr("alt");
                                    image.height = e.attr("height");
                                    image.width = e.attr("width");
                                    image.pageUrl = r.getHeader().getUrl();
                                    Element parent = e.parent();
                                    if (parent != null) {
                                        String parentText = parent.text();
                                        if (parentText != null && !StringUtils.isEmpty(parentText)) {
                                            int limit = parentText.length()>500?500:parentText.length();
                                            image.parentTxt = parent.text().substring(0, limit);
                                        }

                                    }
                                    JSONObject object = new JSONObject(image);
                                    context.write(new Text(src), new Text(object.toString() + ','));
                                }
                            }
                        }
                    }
                } catch (Exception ex) {
                    LOG.error("Caught Exception", ex);
                    context.getCounter(MAPPERCOUNTER.EXCEPTIONS).increment(1);
                }
        }
    }

    public static void main(String[] args) throws Exception {
        File warcFile = new File("/home/kandreadou/Downloads/CC-MAIN-20140707234000-00023-ip-10-180-212-248.ec2.internal.warc.gz");
        Iterator<ArchiveRecord> archIt = WARCReaderFactory.get(warcFile).iterator();
        while (archIt.hasNext()) {
            ArchiveRecord r = archIt.next();
            try {
                System.out.println(r.getHeader().getUrl() + " -- " + r.available());
                // We're only interested in processing the responses, not requests or metadata
                if (r.getHeader().getMimetype().equals("application/http; msgtype=response")) {
                    // Convenience function that reads the full message into a raw byte array
                    byte[] rawData = IOUtils.toByteArray(r, r.available());
                    String content = new String(rawData);
                    String body = content.substring(content.indexOf("\r\n\r\n") + 4);
                    Document doc = Jsoup.parse(content);

                    Elements mf = doc.getElementsByTag("img");

                    if (mf.size() > 0) {
                        for (Element e : mf) {
                            String src = e.attr("src");

                            if (src != null && !StringUtils.isEmpty(src)) {
                                CCImage image = new CCImage();
                                image.src = src;
                                image.alt = e.attr("alt");
                                image.height = e.attr("height");
                                image.width = e.attr("width");
                                image.pageUrl = r.getHeader().getUrl();
                                Element parent = e.parent();
                                if (parent != null) {
                                    String parentText = parent.text();
                                    if (parentText != null && !StringUtils.isEmpty(parentText)) {
                                        int limit = parentText.length()>500?500:parentText.length();
                                        image.parentTxt = parent.text().substring(0, limit);
                                    }

                                }
                                JSONObject object = new JSONObject(image);
                                System.out.println(object.toString() + ',');
                            }
                        }
                    }
                }
            } catch (Exception ex) {
                System.out.println("Caught Exception"+ex);
                //context.getCounter(MAPPERCOUNTER.EXCEPTIONS).increment(1);
            }
        }
    }
}
