package org.commoncrawl.mklab.analysis;

import org.apache.commons.lang.ArrayUtils;
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
    private final static int STEP = 10000;
    private final static int START = 500001;
    private final static int END = 1000001;

    public final static String DOWNLOAD_FOLDER = "/media/kandreadou/New Volume/Pics/";
    private final ImageDAO dao = new ImageDAO();

    private final void dostuff() throws IOException {
        createTrainingFile();

        /*List<CrawledImage> list = dao.findRange(100, 200);
        for (CrawledImage i : list) {

            int[] fVector = createFeatureVector(i);
            System.out.println(ArrayUtils.toString(fVector));

        }*/
    }

    private void createTrainingFile() throws IOException {
        File arffFile = new File("/home/kandreadou/Documents/cctest.arff");
        FileWriter fw = new FileWriter(arffFile.getAbsoluteFile());
        BufferedWriter bw = new BufferedWriter(fw);
        bw.write("@relation banners");
        bw.newLine();
        bw.newLine();
        bw.write("@attribute suffix { jpeg, png, bmp, gif, tiff, other }");
        bw.newLine();
        bw.write("@attribute name { avatar, icon, thumb, tmb, nothing }");
        bw.newLine();
        bw.write("@attribute path { images, pictures, media, pics, gallery, photos, nothing }");
        bw.newLine();
        bw.write("@attribute domDepth numeric");
        bw.newLine();
        bw.write("@attribute domSiblings numeric");
        bw.newLine();
        bw.write("@attribute width numeric");
        bw.newLine();
        bw.write("@attribute height numeric");
        bw.newLine();
        bw.write("@attribute samedomain { TRUE, FALSE }");
        bw.newLine();
        bw.write("@attribute domElement { img, link, a, embed, iframe, object }");
        bw.newLine();
        bw.write("@attribute class {'SMALL','BIG'}");
        bw.newLine();
        bw.newLine();
        bw.write("@data");
        bw.newLine();

        for (int k = START; k < END; k += STEP) {
            List<CrawledImage> list = dao.findRange(k, STEP);
            for (CrawledImage i : list) {
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
        System.out.println(i.normalizedSrc);
        String imName = getImageName(i.normalizedSrc);
        String suffix = getSuffix(imName);
        System.out.println(suffix);

        if ("jpg".equals(suffix) || (!"png".equals(suffix) && !"bmp".equals(suffix) && !"tiff".equals(suffix) && !"gif".equals(suffix)))
            suffix = "jpeg";
        Dimension dim = readFromFilewithImageReader(new File(DOWNLOAD_FOLDER + i.id + "." + suffix));
        if (dim != null) {
            double w = dim.getWidth();
            double h = dim.getHeight();
            if (w < 200 && h < 200)
                isSmall = true;
            if (w > 400 && h > 400)
                isBig = true;
            if (!isSmall && !isBig)
                return false;
        }

        if (!"png".equals(suffix) && !"bmp".equals(suffix) && !"tiff".equals(suffix) && !"gif".equals(suffix) && !"jpeg".equals(suffix))
            bw.write("other,");
        else
            bw.write(suffix + ',');

        String imageNamelc = imName.toLowerCase();
        if (imageNamelc.contains("avatar"))
            bw.write("avatar,");
        else if (imageNamelc.contains("icon"))
            bw.write("icon,");
        else if (imageNamelc.contains("thumb"))
            bw.write("thumb,");
        else if (imageNamelc.contains("tmb"))
            bw.write("tmb,");
        else
            bw.write("nothing,");

        String imUrllc = imName.toLowerCase();
        if (imUrllc.contains("images"))
            bw.write("images,");
        else if (imUrllc.contains("pictures"))
            bw.write("images,");
        else if (imUrllc.contains("media"))
            bw.write("media,");
        else if (imUrllc.contains("pics"))
            bw.write("pics,");
        else if (imUrllc.contains("gallery"))
            bw.write("gallery,");
        else if (imUrllc.contains("photos"))
            bw.write("photos,");
        else
            bw.write("nothing,");

        bw.write(i.domDepth + ",");
        bw.write(i.domSib + ",");

        //estimated dimensions from url
        int[] dims = extractNumeric(i.normalizedSrc);
        bw.write(dims[0] + ",");
        bw.write(dims[1] + ",");

        try {
            URL page = new URL(imUrl);
            String imHost = page.getHost();
            page = new URL(i.pageUrl);
            String pageHost = page.getHost();
            bw.write(imHost.equalsIgnoreCase(pageHost) ? "TRUE" : "FALSE" + ',');
        } catch (MalformedURLException ex) {
            System.out.println(ex);
            bw.write("FALSE,");
        }

        bw.write(i.domElem + ',');
        bw.write(isSmall ? "SMALL" : "BIG");
        return true;
    }

    private int[] createFeatureVector(CrawledImage i) {
        int[] fVector = new int[28];
        String imUrl = i.normalizedSrc;
        System.out.println(i.normalizedSrc);
        String imName = getImageName(i.normalizedSrc);
        String suffix = getSuffix(imName);
        System.out.println(suffix);

        //suffix
        if ("jpg".equalsIgnoreCase(suffix) || "jpeg".equalsIgnoreCase(suffix))
            fVector[0] = 1;
        else if ("bmp".equalsIgnoreCase(suffix))
            fVector[1] = 1;
        else if ("png".equalsIgnoreCase(suffix))
            fVector[2] = 1;
        else if ("gif".equalsIgnoreCase(suffix))
            fVector[3] = 1;
        else if ("tiff".equalsIgnoreCase(suffix))
            fVector[4] = 1;

        //image name contains keywords
        if (imName.toLowerCase().contains("avatar"))
            fVector[5] = 1;
        if (imName.toLowerCase().contains("icon"))
            fVector[6] = 1;
        if (imName.toLowerCase().contains("thumb"))
            fVector[7] = 1;
        if (imName.toLowerCase().contains("tmb"))
            fVector[8] = 1;
        if (imName.toLowerCase().contains("down"))
            fVector[9] = 1;
        if (imName.toLowerCase().contains("up"))
            fVector[10] = 1;

        //image url contains path
        if (imUrl.toLowerCase().contains("/images/"))
            fVector[11] = 1;
        if (imUrl.toLowerCase().contains("/pictures/"))
            fVector[12] = 1;
        if (imUrl.toLowerCase().contains("/media/"))
            fVector[13] = 1;
        if (imUrl.toLowerCase().contains("/pics/"))
            fVector[14] = 1;
        if (imUrl.toLowerCase().contains("/gallery/"))
            fVector[15] = 1;
        if (imUrl.toLowerCase().contains("/photos/"))
            fVector[16] = 1;

        //dom depth
        fVector[17] = i.domDepth;

        //dom siblings
        fVector[18] = i.domSib;

        //estimated dimensions from url
        int[] dims = extractNumeric(i.normalizedSrc);
        fVector[19] = dims[0];
        fVector[20] = dims[1];
        try {
            URL page = new URL(imUrl);
            String imHost = page.getHost();
            page = new URL(i.pageUrl);
            String pageHost = page.getHost();
            fVector[21] = imHost.equalsIgnoreCase(pageHost) ? 1 : 0;
        } catch (MalformedURLException ex) {
            System.out.println(ex);
        }
        if ("img".equals(i.domElem))
            fVector[22] = 1;
        if ("link".equals(i.domElem))
            fVector[23] = 1;
        if ("a".equals(i.domElem))
            fVector[24] = 1;
        if ("embed".equals(i.domElem))
            fVector[25] = 1;
        if ("iframe".equals(i.domElem))
            fVector[26] = 1;
        if ("object".equals(i.domElem))
            fVector[27] = 1;
        try {
            if ("jpg".equals(suffix) || (!"png".equals(suffix) && !"bmp".equals(suffix) && !"tiff".equals(suffix) && !"gif".equals(suffix)))
                suffix = "jpeg";
            Dimension dim = readFromFilewithImageReader(new File(DOWNLOAD_FOLDER + i.id + "." + suffix));
            if (dim != null)
                System.out.println(dim.getWidth() + " " + dim.getHeight());
        } catch (IOException ex) {
            System.out.println(ex);
        }
        return fVector;
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

    public static void main(String[] args) throws Exception {
        MorphiaManager.setup("commoncrawl");
        FilenameAnalyzer f = new FilenameAnalyzer();
        f.dostuff();
        MorphiaManager.tearDown();
    }
}
