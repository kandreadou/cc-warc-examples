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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

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
        a.createTfIdf("/home/kandreadou/Documents/commoncrawlstuff/ngrams_freq_url_big1.txt", "/home/kandreadou/Documents/commoncrawlstuff/ngrams_freq_url_small1.txt");
        //a.createFinalFrequencies("/home/kandreadou/Documents/commoncrawlstuff/ngrams_freq_url_big1.txt", "/home/kandreadou/Documents/commoncrawlstuff/ngrams_freq_url_small1.txt");
        //a.createNgramFile("/home/kandreadou/Documents/commoncrawlstuff/ngrams_freq_url_big1.txt", "/home/kandreadou/Documents/commoncrawlstuff/ngrams_freq_url_small1.txt");
        MorphiaManager.tearDown();
    }

    ///////////////////////////////////////////////////////
    //////////////// CREATE THE FINAL FILE  ///////////////
    ///////////////////////////////////////////////////////

    private void createTfIdf(String fileBig, String fileSmall) throws Exception {
        Multiset<String> big = ConcurrentHashMultiset.create();
        Multiset<String> small = ConcurrentHashMultiset.create();

        readNgramsFromFile(fileBig, true);
        readNgramsFromFile(fileSmall, false);

        //big
        for (Multiset.Entry<String> entry : NGRAM_BIG_FREQUENCIES.entrySet()) {
            String element = entry.getElement();
            int bigCount = entry.getCount();
            if (bigCount < 500)
                continue;
            //System.out.println("Entry " + element + " count " + bigCount);
            if (NGRAM_SMALL_FREQUENCIES.contains(element)) {
                int smallCount = NGRAM_SMALL_FREQUENCIES.count(element);
                int score = (int) ((Math.pow(bigCount, 2) - Math.pow(smallCount, 2)) / (bigCount));
                //System.out.println("Small count " + smallCount + " score " + score);
                if (score > 0)
                    big.add(element, score);
            } else if (bigCount > 3000)
                big.add(element, 1000);
        }

        //small
        for (Multiset.Entry<String> entry : NGRAM_SMALL_FREQUENCIES.entrySet()) {
            String element = entry.getElement();
            int bigCount = entry.getCount();
            if (bigCount < 500)
                continue;
            //System.out.println("Entry " + element + " count " + bigCount);
            if (NGRAM_BIG_FREQUENCIES.contains(element)) {
                int smallCount = NGRAM_BIG_FREQUENCIES.count(element);
                int score = (int) ((Math.pow(bigCount, 2) - Math.pow(smallCount, 2)) / (bigCount));
                //System.out.println("Small count " + smallCount + " score " + score);
                if (score > 0)
                    small.add(element, score);
            } else if (bigCount > 3000)
                small.add(element, 1000);
        }

        Iterable<Multiset.Entry<String>> bigmultiset =
                Multisets.copyHighestCountFirst(big).entrySet();
        Iterable<Multiset.Entry<String>> smallmultiset =
                Multisets.copyHighestCountFirst(small).entrySet();
        PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter("/home/kandreadou/Documents/commoncrawlstuff/ngrams_tfidf_1000_beta.txt", false)));
        int count = 0;
        for (Multiset.Entry<String> s : bigmultiset) {
            if (count >= 500)
                break;
            System.out.println(s.getElement() + " " + s.getCount());
            writer.println(s.getElement() + " " + s.getCount());
            count++;
        }
        count = 0;
        for (Multiset.Entry<String> s : smallmultiset) {
            if (count >= 500)
                break;
            System.out.println(s.getElement() + " " + s.getCount());
            writer.println(s.getElement() + " " + s.getCount());
            count++;
        }
        writer.close();
    }

    private void createFinalFrequencies(String fileBig, String fileSmall) throws Exception {
        Multiset<String> big = ConcurrentHashMultiset.create();
        Multiset<String> small = ConcurrentHashMultiset.create();

        readNgramsFromFile(fileBig, true);
        readNgramsFromFile(fileSmall, false);

        //big
        for (Multiset.Entry<String> entry : NGRAM_BIG_FREQUENCIES.entrySet()) {
            String element = entry.getElement();
            int bigCount = entry.getCount();
            //System.out.println("Entry " + element + " count " + bigCount);
            if (NGRAM_SMALL_FREQUENCIES.contains(element)) {
                int smallCount = NGRAM_SMALL_FREQUENCIES.count(element);
                int score = bigCount - smallCount;
                //System.out.println("Small count " + smallCount + " score " + score);
                if (score > 0)
                    big.add(element, score);
            } else
                big.add(element, bigCount);
        }

        //small
        for (Multiset.Entry<String> entry : NGRAM_SMALL_FREQUENCIES.entrySet()) {
            String element = entry.getElement();
            int bigCount = entry.getCount();
            //System.out.println("Entry " + element + " count " + bigCount);
            if (NGRAM_BIG_FREQUENCIES.contains(element)) {
                int smallCount = NGRAM_BIG_FREQUENCIES.count(element);
                int score = bigCount - smallCount;
                //System.out.println("Small count " + smallCount + " score " + score);
                if (score > 0)
                    small.add(element, score);
            } else
                small.add(element, bigCount);
        }

        Iterable<Multiset.Entry<String>> bigmultiset =
                Multisets.copyHighestCountFirst(big).entrySet();
        Iterable<Multiset.Entry<String>> smallmultiset =
                Multisets.copyHighestCountFirst(small).entrySet();
        PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter("/home/kandreadou/Documents/commoncrawlstuff/ngrams_scores_5000.txt", false)));
        int count = 0;
        for (Multiset.Entry<String> s : bigmultiset) {
            if (count >= 2500)
                break;
            System.out.println(s.getElement() + " " + s.getCount());
            writer.println(s.getElement() + " " + s.getCount());
            count++;
        }
        count = 0;
        for (Multiset.Entry<String> s : smallmultiset) {
            if (count >= 2500)
                break;
            System.out.println(s.getElement() + " " + s.getCount());
            writer.println(s.getElement() + " " + s.getCount());
            count++;
        }
        writer.close();
    }


    private void readNgramsFromFile(String file, boolean isBig) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
        String line = reader.readLine();
        while (line != null) {
            line = line.trim();
            int lastSpace = line.lastIndexOf(' ');
            if (lastSpace > 0) {
                String frequency = line.substring(lastSpace + 1);
                String element = line.substring(0, lastSpace);
                if (isBig)
                    NGRAM_BIG_FREQUENCIES.add(element, Integer.parseInt(frequency));
                else
                    NGRAM_SMALL_FREQUENCIES.add(element, Integer.parseInt(frequency));
            }
            line = reader.readLine();
        }
        reader.close();
        reader = null;

    }

    ///////////////////////////////////////////////////////
    //////////////// CREATE THE PRELIMINARY FILES  ////////
    ///////////////////////////////////////////////////////

    private void createNgramFile(String filename1, String filename2) throws IOException {

        extractNgramsFromURLNoFiles();

        Iterable<Multiset.Entry<String>> multiset =
                Multisets.copyHighestCountFirst(NGRAM_BIG_FREQUENCIES).entrySet();
        PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(filename1, false)));
        for (Multiset.Entry<String> s : multiset) {
            if (s.getCount() < 50)
                break;
            writer.println(s.getElement() + " " + s.getCount());
        }
        writer.close();

        Iterable<Multiset.Entry<String>> multiset2 =
                Multisets.copyHighestCountFirst(NGRAM_SMALL_FREQUENCIES).entrySet();
        PrintWriter writer2 = new PrintWriter(new BufferedWriter(new FileWriter(filename2, false)));
        for (Multiset.Entry<String> s : multiset2) {
            if (s.getCount() < 50)
                break;
            writer2.println(s.getElement() + " " + s.getCount());
        }
        writer2.close();
    }

    private void extractNgramsFromURLNoFiles() {
        for (int k = START; k < END; k += STEP) {
            System.out.println(k);
            System.out.println("BIG " + NUM_BIG + " SMALL " + NUM_SMALL);
            List<CrawledImage> list = dao.findRange(k, STEP);
            for (CrawledImage i : list) {

                //System.out.println("ID "+i.id);
                if (!i.isSmall && !i.isBig)
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


    private void extractNgramsFromURL() {
        for (int k = START; k < END; k += STEP) {
            System.out.println(k);
            System.out.println("BIG " + NUM_BIG + " SMALL " + NUM_SMALL);
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
            if (isBig)
                NGRAM_BIG_FREQUENCIES.add(token);
            else
                NGRAM_SMALL_FREQUENCIES.add(token);
        }

        gramTokenizer.end();
        gramTokenizer.close();
        reader.close();
    }

}
