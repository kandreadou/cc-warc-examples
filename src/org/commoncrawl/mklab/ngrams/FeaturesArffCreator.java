package org.commoncrawl.mklab.ngrams;

import org.apache.commons.lang.StringUtils;
import org.commoncrawl.mklab.analysis.CrawledImage;
import org.commoncrawl.mklab.ngrams.IArffCreator;

import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;
import java.awt.*;
import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by kandreadou on 12/3/14.
 */
public class FeaturesArffCreator extends IArffCreator {

    public FeaturesArffCreator(String filename) throws IOException {
        super(filename);
    }

    @Override
    public void initialize() throws IOException {
        bw.write("@relation banners");
        bw.newLine();
        bw.newLine();
        bw.write("@attribute suffix_JEPG numeric");
        bw.newLine();
        bw.write("@attribute suffix_PNG numeric");
        bw.newLine();
        bw.write("@attribute suffix_BMP numeric");
        bw.newLine();
        bw.write("@attribute suffix_GIF numeric");
        bw.newLine();
        bw.write("@attribute suffix_TIFF numeric");
        bw.newLine();
        bw.write("@attribute domDepth numeric");
        bw.newLine();
        bw.write("@attribute domSiblings numeric");
        bw.newLine();
        bw.write("@attribute hasWidth numeric");
        bw.newLine();
        bw.write("@attribute width numeric");
        bw.newLine();
        bw.write("@attribute hasHeight numeric");
        bw.newLine();
        bw.write("@attribute height numeric");
        bw.newLine();
        bw.write("@attribute samedomain numeric");
        bw.newLine();
        bw.write("@attribute domElement_IMG numeric");
        bw.newLine();
        bw.write("@attribute domElement_LINK numeric");
        bw.newLine();
        bw.write("@attribute domElement_A numeric");
        bw.newLine();
        bw.write("@attribute domElement_EMBED numeric");
        bw.newLine();
        bw.write("@attribute domElement_IFRAME numeric");
        bw.newLine();
        bw.write("@attribute domElement_OBJECT numeric");
        bw.newLine();
        bw.write("@attribute hasAltText numeric");
        bw.newLine();
        bw.write("@attribute altTextLength numeric");
        bw.newLine();
        bw.write("@attribute hasParentText numeric");
        bw.newLine();
        bw.write("@attribute parentTextLength numeric");
        bw.newLine();
        bw.write("@attribute urlLength numeric");
        bw.newLine();
        bw.write("@attribute class {'SMALL','BIG'}");
        bw.newLine();
        bw.newLine();
        bw.write("@data");
        bw.newLine();
    }

    @Override
    public void writeFeatureVector(CrawledImage i, boolean isBig) throws IOException {
        writeFeatureVector1(i, isBig);
        bw.newLine();
    }

    @Override
    public void teardown() throws IOException {
        bw.close();
        fw.close();
    }

    private void writeFeatureVector1(CrawledImage i, boolean isBig) throws IOException {

        String imUrl = i.normalizedSrc;
        //System.out.println(i.normalizedSrc);
        String imName = getImageName(i.normalizedSrc);
        String suffix = getSuffix(imName);
        //System.out.println(suffix);

        bw.write(String.valueOf("jpeg".equals(suffix) ? 1 : 0) + ',');
        bw.write(String.valueOf("png".equals(suffix) ? 1 : 0) + ',');
        bw.write(String.valueOf("bmp".equals(suffix) ? 1 : 0) + ',');
        bw.write(String.valueOf("gif".equals(suffix) ? 1 : 0) + ',');
        bw.write(String.valueOf("tiff".equals(suffix) ? 1 : 0) + ',');

        bw.write(String.valueOf(i.domDepth) + ',');
        bw.write(String.valueOf(i.domSib) + ',');

        //estimated dimensions from url
        int[] dims = extractNumeric(i.normalizedSrc);
        bw.write(String.valueOf(dims[0] > 0 ? 1 : 0) + ',');
        bw.write(String.valueOf(dims[0]) + ',');
        bw.write(String.valueOf(dims[1] > 0 ? 1 : 0) + ',');
        bw.write(String.valueOf(dims[1]) + ',');

        try {
            URL page = new URL(imUrl);
            String imHost = page.getHost();
            page = new URL(i.pageUrl);
            String pageHost = page.getHost();
            bw.write(String.valueOf(imHost.equalsIgnoreCase(pageHost) ? 1 : 0) + ',');
        } catch (MalformedURLException ex) {
            System.out.println(ex);
            bw.write("0,");
        }

        bw.write(String.valueOf("img".equals(i.domElem) ? 1 : 0) + ',');
        bw.write(String.valueOf("link".equals(i.domElem) ? 1 : 0) + ',');
        bw.write(String.valueOf("a".equals(i.domElem) ? 1 : 0) + ',');
        bw.write(String.valueOf("embed".equals(i.domElem) ? 1 : 0) + ',');
        bw.write(String.valueOf("iframe".equals(i.domElem) ? 1 : 0) + ',');
        bw.write(String.valueOf("object".equals(i.domElem) ? 1 : 0) + ',');

        if (StringUtils.isEmpty(i.alt)) {
            bw.write("0,0,");
        } else {
            bw.write("1," + i.alt.length() + ",");
        }
        if (StringUtils.isEmpty(i.parentTxt)) {
            bw.write("0,0,");
        } else {
            bw.write("1," + i.parentTxt.length() + ",");
        }
        bw.write(String.valueOf(i.normalizedSrc.length()) + ',');

        bw.write(isBig ? "BIG" : "SMALL");
    }

    public final static int[] extractNumeric(String input) {
        int[] dimensions = new int[2];
        try {
            //Find the following: w_75_ or h_75 or 350x250 or 250px or _75. or (width|height|w|h)=150
            Pattern pattern = Pattern.compile("(\\d+x\\d+)+|(w|h|s)_?\\d+|\\d+px|(width|height|w|h)=\\d+|_\\d+\\.");
            Matcher matcher = pattern.matcher(input);
            boolean found = matcher.find();
            while (found) {
                String element = matcher.group();
                System.out.println(element);
                String[] dims = element.split("x");
                if (dims.length > 1) {
                    dimensions[0] = Integer.parseInt(dims[0]);
                    dimensions[1] = Integer.parseInt(dims[1]);
                    break;
                } else {
                    if (element.contains("w") || element.contains("s"))
                        dimensions[0] = Integer.parseInt(stripNonNumericChars(element));
                    else
                        dimensions[1] = Integer.parseInt(stripNonNumericChars(element));
                }
                found = matcher.find(matcher.end());
            }
        } catch (NumberFormatException ex) {
            System.out.println(ex);
        }
        return dimensions;
    }

    private final static String stripNonNumericChars(String s) {
        return s.replaceAll("[^\\d]", "");
    }

    public final static String getImageName(String url) {
        return url.substring(url.lastIndexOf("/") + 1);
    }

    public final static String getSuffix(String name) {
        return name.substring(name.lastIndexOf(".") + 1);
    }
}
