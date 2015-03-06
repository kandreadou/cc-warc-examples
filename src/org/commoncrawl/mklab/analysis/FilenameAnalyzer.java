package org.commoncrawl.mklab.analysis;

import org.apache.commons.lang.StringUtils;

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
public class FilenameAnalyzer {

    private final static String[] IMAGE_NAME_FILTER = {"avatar", "icon", "thumb", "tmb", "up", "down"};
    private final static String[] URL_PATH_FILTER = {"Images/", "/Pictures/", "/Media/", "/Gallery/", "/Pics/", "/Photos/"};
    private final static int STEP = 1000;
    private final static int START = 0;
    private final static int END = 10000;
    private final static int MAX_BIG = 500;
    private final static int MAX_SMALL = 500;

    public final static String DOWNLOAD_FOLDER2 = "/media/kandreadou/New Volume/Pics2/";
    private final ImageDAO dao = new ImageDAO();

    private int NUM_BIG = 0;
    private int NUM_SMALL = 0;

    public static void main(String[] args) throws Exception {
        MorphiaManager.setup("commoncrawl2");
        FilenameAnalyzer f = new FilenameAnalyzer();
        f.createTrainingFile("/home/kandreadou/Documents/commoncrawlstuff/training_data/justatest.arff");
        MorphiaManager.tearDown();
    }

    private void createTrainingFile(String filname) throws IOException {
        File arffFile = new File(filname);
        FileWriter fw = new FileWriter(arffFile.getAbsoluteFile());
        BufferedWriter bw = new BufferedWriter(fw);
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

        for (int k = START; k < END; k += STEP) {
            List<CrawledImage> list = dao.findRange(k, STEP);
            for (CrawledImage i : list) {
                if (NUM_SMALL > MAX_SMALL && NUM_BIG > MAX_BIG)
                    break;
                try {
                    if (writeFeatureVector(i, bw))
                        bw.newLine();
                } catch (IOException ioe) {
                    System.out.println(ioe);
                }
            }
        }
        bw.close();
        fw.close();
    }

    private boolean writeFeatureVector(CrawledImage i, BufferedWriter bw) throws IOException {

        boolean isSmall = false;
        boolean isBig = false;
        String imUrl = i.normalizedSrc;
        //System.out.println(i.normalizedSrc);
        String imName = getImageName(i.normalizedSrc);
        String suffix = getSuffix(imName);
        //System.out.println(suffix);

        Dimension dim = readFromFilewithImageReader(new File(DOWNLOAD_FOLDER2 + i.filename));
        if (dim == null)
            return false;
        else {
            double w = dim.getWidth();
            double h = dim.getHeight();
            if (w < 200 && h < 200)
                isSmall = true;
            if (w > 400 && h > 400)
                isBig = true;
            if (!isSmall && !isBig)
                return false;
            if (isSmall && NUM_SMALL > MAX_SMALL)
                return false;
            else if (isBig && NUM_BIG > MAX_BIG)
                return false;
            else {
                if (isSmall)
                    NUM_SMALL++;
                else
                    NUM_BIG++;
                System.out.println("SMALL " + NUM_SMALL + " BIG " + NUM_BIG);
            }
        }

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

        bw.write(isSmall ? "SMALL" : "BIG");

        return true;
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

    //Approximate duration 120 ms
    private Dimension readFromURLwithImageReader(URL url) throws IOException {
        ImageInputStream in = ImageIO.createImageInputStream(url.openStream());
        try {
            final Iterator<ImageReader> readers = ImageIO.getImageReaders(in);
            if (readers.hasNext()) {
                ImageReader reader = readers.next();
                try {
                    reader.setInput(in);
                    return new Dimension(reader.getWidth(0), reader.getHeight(0));
                } finally {
                    reader.dispose();
                }
            }
        } finally {
            if (in != null) in.close();
        }
        return null;
    }

    //Approximate duration 1 ms
    public static Dimension readFromFilewithImageReader(File file) throws IOException {
        ImageInputStream in = ImageIO.createImageInputStream(new FileInputStream(file));
        try {
            final Iterator<ImageReader> readers = ImageIO.getImageReaders(in);
            if (readers.hasNext()) {
                ImageReader reader = readers.next();
                try {
                    reader.setInput(in);
                    return new Dimension(reader.getWidth(0), reader.getHeight(0));
                } catch (Exception ex) {
                    System.out.println(ex);
                } finally {
                    reader.dispose();
                }
            }
        } finally {
            if (in != null) in.close();
        }
        return null;
    }
}
