package org.commoncrawl.mklab.analysis;

import org.apache.commons.lang.ArrayUtils;

import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;
import java.awt.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
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
    private final static String[] URL_PATH_FILTER = {"/Images/", "/Pictures/", "/Media/", "/Gallery/", "/Pics/", "/Photos/"};

    private final static String DOWNLOAD_FOLDER = "/media/kandreadou/New Volume/Pics/";
    private final ImageDAO dao = new ImageDAO();

    private final void dostuff() throws IOException {
        int[] fVector = new int[50];
        List<CrawledImage> list = dao.findRange(100, 200);
        for (CrawledImage i : list) {
            String imUrl = i.normalizedSrc;
            System.out.println(i.normalizedSrc);
            String imName = getImageName(i.normalizedSrc);
            //System.out.println(imageName);
            String suffix = getSuffix(imName);
            if("jpg".equalsIgnoreCase(suffix)||"jpeg".equalsIgnoreCase(suffix))
                fVector[0]=1;
            else if("bmp".equalsIgnoreCase(suffix))
                fVector[1] =1;
            else if("png".equalsIgnoreCase(suffix))
                fVector[2] =1;
            else if("gif".equalsIgnoreCase(suffix))
                fVector[3] =1;
            else if("tiff".equalsIgnoreCase(suffix))
                fVector[4] =1;
            if (imName.contains("avatar"))
                fVector[5] =1;
            if (imName.contains("icon"))
                fVector[6] =1;
            if (imName.contains("thumb"))
                fVector[7] =1;
            if (imName.contains("tmb"))
                fVector[8] =1;
            if (imName.contains("down"))
                fVector[9] =1;
            if (imName.contains("up"))
                fVector[10] =1;
            if(imUrl.toLowerCase().contains("/images/"))
                fVector[11] =1;
            if(imUrl.toLowerCase().contains("/pictures/"))
                fVector[12] =1;
            if(imUrl.toLowerCase().contains("/media/"))
                fVector[13] =1;
            if(imUrl.toLowerCase().contains("/pics/"))
                fVector[14] =1;
            if(imUrl.toLowerCase().contains("/gallery/"))
                fVector[15] =1;
            if(imUrl.toLowerCase().contains("/photos/"))
                fVector[16] =1;

            fVector[17] = i.domDepth;
            fVector[18] = i.domSib;
            int[] dims = extractNumeric(i.normalizedSrc);
            fVector[19] = dims[0];
            fVector[20] = dims[1];
            //TODO: same host domain
            //TODO: type of DOM element

            System.out.println(ArrayUtils.toString(fVector));

        }
    }

    public final static int[] extractNumeric(String input) {
        int[] dimensions = new int[2];
        //Find the following: w_75_ or h_75 or 350x250 or 250px or _75. or (width|height|w|h)=150
        Pattern pattern = Pattern.compile("(\\d+x\\d+)+|w_\\d+|h_\\d+|\\d+px|(width|height|w|h)=\\d+|_\\d+\\.");
        Matcher matcher = pattern.matcher(input);
        boolean found = matcher.find();
        while (found) {
            String element = matcher.group();
            System.out.println(element);
            String[] dims = element.split("x");
            if(dims.length>1){
                dimensions[0] = Integer.parseInt(dims[0]);
                dimensions[1] = Integer.parseInt(dims[1]);
                break;
            }else{
                if(element.contains("w"))
                    dimensions[0] =  Integer.parseInt(stripNonNumericChars(element));
                else
                    dimensions[1] =  Integer.parseInt(stripNonNumericChars(element));
            }
            found = matcher.find(matcher.end());
        }
        return dimensions;
    }

    private final static String stripNonNumericChars(String s){
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
    private Dimension readFromFilewithImageReader(File file) throws IOException {
        ImageInputStream in = ImageIO.createImageInputStream(new FileInputStream(file));
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

    public static void main(String[] args) throws Exception {
        MorphiaManager.setup("commoncrawl");
        FilenameAnalyzer f = new FilenameAnalyzer();
        f.dostuff();
        MorphiaManager.tearDown();
    }
}
