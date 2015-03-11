package org.commoncrawl.mklab.ngrams;

import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.Multiset;
import com.google.common.collect.Multisets;
import org.apache.lucene.analysis.ngram.NGramTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.commoncrawl.mklab.analysis.CrawledImage;
import org.commoncrawl.mklab.analysis.ImageDAO;
import org.commoncrawl.mklab.analysis.MorphiaManager;

import java.awt.*;
import java.io.*;
import java.net.URLDecoder;
import java.util.List;

/**
 * Created by kandreadou on 3/11/15.
 */
public class NgamFeatureSelection {

    public final static String DOWNLOAD_FOLDER = "/media/kandreadou/New Volume/Pics_train/";
    public final static int MIN_NGRAM_SIZE = 3;
    public final static int MAX_NGRAM_SIZE = 5;
    private final ImageDAO dao = new ImageDAO();
    private final static int STEP = 10000;
    private final static int START = 0;
    private final static int END = 4600000;

    private static int NUM_BIG = 0;
    private static int NUM_SMALL = 0;

    public static Multiset<String> NGRAM_BIG_FREQUENCIES = ConcurrentHashMultiset.create();
    public static Multiset<String> NGRAM_SMALL_FREQUENCIES = ConcurrentHashMultiset.create();

    public static void main(String[] args) throws Exception {
        MorphiaManager.setup("cc_train");
        NgamFeatureSelection a = new NgamFeatureSelection();
        a.createNgramFile("/home/kandreadou/Documents/commoncrawlstuff/ngrams_freq_url_big.txt", "/home/kandreadou/Documents/commoncrawlstuff/ngrams_freq_url_small.txt");
        MorphiaManager.tearDown();
    }

    ///////////////////////////////////////////////////////
    //////////////// NGRAM FILE SECTION   /////////////////
    ///////////////////////////////////////////////////////

    private void createNgramFile(String filename1, String filename2) throws IOException {

        extractNgramsFromURL();

        Iterable<Multiset.Entry<String>> multiset =
                Multisets.copyHighestCountFirst(NGRAM_BIG_FREQUENCIES).entrySet();
        PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(filename1, false)));
        for (Multiset.Entry<String> s : multiset) {
            if (s.getCount() < 1000)
                break;
            writer.println(s.getElement() + " " + s.getCount());
        }
        writer.close();

        Iterable<Multiset.Entry<String>> multiset2 =
                Multisets.copyHighestCountFirst(NGRAM_SMALL_FREQUENCIES).entrySet();
        PrintWriter writer2 = new PrintWriter(new BufferedWriter(new FileWriter(filename2, false)));
        for (Multiset.Entry<String> s : multiset2) {
            if (s.getCount() < 1000)
                break;
            writer2.println(s.getElement() + " " + s.getCount());
        }
        writer2.close();
    }

    private void extractNgramsFromURL() {
        for (int k = START; k < END; k += STEP) {
            System.out.println(k);
            System.out.println("BIG "+NUM_BIG+" SMALL "+NUM_SMALL);
            List<CrawledImage> list = dao.findRange(k, STEP);
            for (CrawledImage i : list) {
                //System.out.println("ID "+i.id);
                try {
                    Dimension dim = ArffController.readFromFilewithImageReader(new File(DOWNLOAD_FOLDER + i.filename));
                    if (dim == null)
                        continue;
                    else {
                        double w = dim.getWidth();
                        double h = dim.getHeight();
                        if (w < 200 && h < 200)
                            i.isSmall = true;
                        if (w > 400 && h > 400)
                            i.isBig = true;
                        if (!i.isSmall && !i.isBig)
                            continue;
                        else
                            dao.save(i);
                        /*if (isSmall && NUM_SMALL > MAX_SMALL)
                            continue;
                        else if (isBig && NUM_BIG > MAX_BIG)
                            continue;*/
                        if (i.isSmall && NUM_SMALL > NUM_BIG)
                            continue;
                        else {
                            if (i.isSmall)
                                NUM_SMALL++;
                            else
                                NUM_BIG++;

                        }
                    }
                    processURL(i.normalizedSrc, i.isBig);
                } catch (IOException ioe) {

                }
            }
        }
    }


    private void processURL(String s, boolean isBig) {
        try {
            s = URLDecoder.decode(s, "UTF-8");
        } catch (Exception uee) {
            System.out.println("Wrong encoding " + uee);
        }
        //System.out.println("** " + s);
        int start = 0;
        if (s.startsWith("http"))
            start = s.indexOf("://") + 3;
        s = s.substring(start);
        // We assume everything that is between the last dot and the end of the URL
        // and is 3 or 4 characters long is the extension (e.g. .html, .htm, .jpeg, .pdf)
        int lastIndexOfDot = s.lastIndexOf('.');
        if (lastIndexOfDot >= s.length() - 5)
            s = s.substring(0, lastIndexOfDot);
        // Split on dot or forward slash or -
        String[] items = s.split("/|\\.|\\-");
        //System.out.println(ArrayUtils.toString(items));
        for (String i : items) {
            try {
                extractNGrams(i, isBig);
            } catch (IOException ioe) {
                System.out.println(ioe);
            }
        }
    }

    public void extractNGrams(String input, boolean isBig) throws IOException {
        Reader reader = new StringReader(input);
        NGramTokenizer gramTokenizer = new NGramTokenizer(reader, MIN_NGRAM_SIZE, MAX_NGRAM_SIZE);
        gramTokenizer.reset();
        CharTermAttribute charTermAttribute = gramTokenizer.addAttribute(CharTermAttribute.class);

        while (gramTokenizer.incrementToken()) {
            String token = charTermAttribute.toString();
            if(isBig)
                NGRAM_BIG_FREQUENCIES.add(token);
            else
                NGRAM_SMALL_FREQUENCIES.add(token);
        }

        gramTokenizer.end();
        gramTokenizer.close();
        reader.close();
    }

}
