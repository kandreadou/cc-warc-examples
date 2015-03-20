package org.commoncrawl.mklab.ngrams;

import com.google.common.collect.LinkedHashMultiset;
import com.google.common.collect.Multiset;
import com.google.common.collect.Multisets;
import org.apache.lucene.analysis.ngram.NGramTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.commoncrawl.mklab.analysis.CrawledImage;
import org.commoncrawl.mklab.analysis.ImageDAO;
import org.commoncrawl.mklab.analysis.MorphiaManager;

import java.io.*;
import java.net.URLDecoder;
import java.util.Iterator;
import java.util.List;

/**
 * Created by kandreadou on 1/5/15.
 */
public class NGramAnalyzer {

    public final static int MIN_NGRAM_SIZE = 2;
    public final static int MAX_NGRAM_SIZE = 15;
    private final ImageDAO dao = new ImageDAO();
    private final static int STEP = 10000;
    private final static int START = 0;
    private final static int END = 4000000;

    public static Multiset<String> NGRAM_FREQUENCIES_BIG = LinkedHashMultiset.create(200000);
    public static Multiset<String> NGRAM_FREQUENCIES_SMALL = LinkedHashMultiset.create(200000);

    public static void main(String[] args) throws Exception {
        MorphiaManager.setup("cc_train");
        createNgramFile("/home/kandreadou/Documents/commoncrawlstuff/new/ngrams_big.txt", "/home/kandreadou/Documents/commoncrawlstuff/new/ngrams_small.txt");
        MorphiaManager.tearDown();
    }

    ///////////////////////////////////////////////////////
    //////////////// NGRAM FILE SECTION   /////////////////
    ///////////////////////////////////////////////////////

    private static void createNgramFile(String filenameBig, String filenameSmall) throws IOException {
        NGramAnalyzer a = new NGramAnalyzer();
        a.extractNgramsFromURL();

        System.out.println("Big initial size before "+NGRAM_FREQUENCIES_BIG.size());
        Iterator<Multiset.Entry<String>> iterator = NGRAM_FREQUENCIES_BIG.entrySet().iterator();
        while (iterator.hasNext()) {
                if (iterator.next().getCount()<100)
                    iterator.remove();

        }
        System.out.println("Big initial size after "+NGRAM_FREQUENCIES_BIG.size());
        System.out.println("Big initial size before "+NGRAM_FREQUENCIES_SMALL.size());
        iterator = NGRAM_FREQUENCIES_SMALL.entrySet().iterator();
        while (iterator.hasNext()) {
            if (iterator.next().getCount()<100)
                iterator.remove();
        }
        System.out.println("Big initial size after "+NGRAM_FREQUENCIES_SMALL.size());

        Iterable<Multiset.Entry<String>> multiset =
                Multisets.copyHighestCountFirst(NGRAM_FREQUENCIES_BIG).entrySet();
        PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(filenameBig, false)));
        for (Multiset.Entry<String> s : multiset) {
            //System.out.println(s.getElement() + " " + s.getCount());
            writer.println(s.getElement() + " " + s.getCount());
        }
        writer.close();

        multiset =
                Multisets.copyHighestCountFirst(NGRAM_FREQUENCIES_SMALL).entrySet();
        System.out.println("#########");
        writer = new PrintWriter(new BufferedWriter(new FileWriter(filenameSmall, false)));
        for (Multiset.Entry<String> s : multiset) {
            //System.out.println(s.getElement() + " " + s.getCount());
            writer.println(s.getElement() + " " + s.getCount());
        }
        writer.close();
    }

    private static int NUM_BIG = 0;
    private static int NUM_SMALL = 0;

    private void extractNgramsFromURL() {
        for (int k = START; k < END; k += STEP) {
            System.out.println(k);
            List<CrawledImage> list = dao.findRange(k, STEP);
            for (CrawledImage i : list) {
                if (!i.isBig && !i.isSmall)
                    continue;
                if (i.isSmall && NUM_SMALL > NUM_BIG)
                    continue;
                else {
                    if (i.isSmall)
                        NUM_SMALL++;
                    else
                        NUM_BIG++;
                }
                processURL(i.normalizedSrc, i.isBig);
            }
        }
    }

    public static void processURL(String s, boolean isBig) {
        //System.out.println(s);
        try {
            s = URLDecoder.decode(s, "UTF-8");
        } catch (Exception uee) {
            System.out.println("Wrong encoding " + uee);
        }
        String[] items = s.split("\\W+");
        //System.out.println(ArrayUtils.toString(items));
        for (String i : items) {
            //System.out.println(i);
            try {
                extractNGrams(i, isBig);
            } catch (IOException ioe) {
                System.out.println(ioe);
            }
        }
    }

    /*public static void processURL(String s, boolean isBig) {
        System.out.println(s);
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
        //String[] items = s.split("/|\\.|\\-");
        String[] items = s.split("\\W+");
        //System.out.println(ArrayUtils.toString(items));
        for (String i : items) {
            System.out.println(i);
            try {
                extractNGrams(i, isBig);
            } catch (IOException ioe) {
                System.out.println(ioe);
            }
        }
    }

    private void extractNgramsFromAltText() {
        for (int k = START; k < END; k += STEP) {
            List<CrawledImage> list = dao.findRange(k, STEP);
            for (CrawledImage i : list) {
                if (i.parentTxt != null) {
                    try {
                        extractNGrams(i.parentTxt);
                    } catch (Exception e) {
                        System.out.println(e);
                    }
                }
            }
        }
    }*/

    /**
     * Extracts ngrams from the given input string
     *
     * @param input
     * @throws IOException
     */
    public static void extractNGrams(String input, boolean isBig) throws IOException {
        Reader reader = new StringReader(input);
        NGramTokenizer gramTokenizer = new NGramTokenizer(reader, MIN_NGRAM_SIZE, MAX_NGRAM_SIZE);
        gramTokenizer.reset();
        CharTermAttribute charTermAttribute = gramTokenizer.addAttribute(CharTermAttribute.class);

        while (gramTokenizer.incrementToken()) {
            String token = charTermAttribute.toString();
            if (isBig)
                NGRAM_FREQUENCIES_BIG.add(token);
            else
                NGRAM_FREQUENCIES_SMALL.add(token);
        }

        gramTokenizer.end();
        gramTokenizer.close();
        gramTokenizer = null;
        charTermAttribute = null;
        reader.close();
        reader = null;
    }
}
