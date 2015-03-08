package org.commoncrawl.mklab.ngrams;

import org.commoncrawl.mklab.analysis.CrawledImage;
import org.commoncrawl.mklab.analysis.ImageDAO;
import org.commoncrawl.mklab.analysis.MorphiaManager;

import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;
import java.awt.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by kandreadou on 3/6/15.
 */
public class ArffController {

    public final static String DOWNLOAD_FOLDER = "/media/kandreadou/New Volume/Pics_train/";
    private final static int STEP = 10000;
    private final static int START = 0;
    private final static int END = 4640038;
    //private final static int MAX_BIG = 5000;
    //private final static int MAX_SMALL = 5000;

    private static int NUM_BIG = 0;
    private static int NUM_SMALL = 0;
    private static final List<IArffCreator> creators = new ArrayList<IArffCreator>();

    public static void main(String[] args) throws Exception {
        MorphiaManager.setup("cc_train");
        NGramArffCreator n1000 = new NGramArffCreator("/home/kandreadou/Documents/commoncrawlstuff/ngrams_url_train_1000.arff", 1000);
        NGramArffCreator n2000 = new NGramArffCreator("/home/kandreadou/Documents/commoncrawlstuff/ngrams_url_train_2000.arff", 2000);
        NGramArffCreator n5000 = new NGramArffCreator("/home/kandreadou/Documents/commoncrawlstuff/ngrams_url_train_5000.arff", 5000);
        FeaturesArffCreator f = new FeaturesArffCreator("/home/kandreadou/Documents/commoncrawlstuff/features_train.arff");
        creators.add(n1000);
        creators.add(n2000);
        creators.add(n5000);
        creators.add(f);
        for (IArffCreator c : creators) {
            c.initialize();
        }
        ImageDAO dao = new ImageDAO();
        for (int k = START; k < END; k += STEP) {
            //if (NUM_SMALL > MAX_SMALL && NUM_BIG > MAX_BIG)
            //break;
            System.out.println("K= " + k);
            System.out.println("SMALL " + NUM_SMALL + " BIG " + NUM_BIG);
            List<CrawledImage> list = dao.findRange(k, STEP);
            for (CrawledImage i : list) {
                try {
                    //if (NUM_SMALL > MAX_SMALL && NUM_BIG > MAX_BIG)
                    //break;
                    boolean isSmall = false;
                    boolean isBig = false;
                    Dimension dim = readFromFilewithImageReader(new File(DOWNLOAD_FOLDER + i.filename));
                    if (dim == null)
                        continue;
                    else {
                        double w = dim.getWidth();
                        double h = dim.getHeight();
                        if (w < 200 && h < 200)
                            isSmall = true;
                        if (w > 400 && h > 400)
                            isBig = true;
                        if (!isSmall && !isBig)
                            continue;
                        /*if (isSmall && NUM_SMALL > MAX_SMALL)
                            continue;
                        else if (isBig && NUM_BIG > MAX_BIG)
                            continue;*/
                        if (isSmall && NUM_SMALL > NUM_BIG)
                            continue;
                        else {
                            if (isSmall)
                                NUM_SMALL++;
                            else
                                NUM_BIG++;

                        }
                    }
                    for (IArffCreator c : creators) {
                        c.writeFeatureVector(i, isBig);
                    }

                } catch (IOException ioe) {
                    System.out.println(ioe);
                } catch (NullPointerException npe) {
                    System.out.println(npe);
                }
            }
        }
        for (IArffCreator c : creators) {
            c.teardown();
        }
        MorphiaManager.tearDown();
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
