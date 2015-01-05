package org.commoncrawl.mklab.ngrams;

import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.Multiset;
import com.google.common.collect.Multisets;
import org.apache.lucene.analysis.ngram.NGramTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.commoncrawl.mklab.analysis.CrawledImage;
import org.commoncrawl.mklab.analysis.ImageDAO;
import org.commoncrawl.mklab.analysis.MorphiaManager;

import java.io.*;
import java.util.List;

/**
 * Created by kandreadou on 1/5/15.
 */
public class NGramAnalyzer {

    public final static int MIN_NGRAM_SIZE = 3;
    public final static int MAX_NGRAM_SIZE = 5;
    private final ImageDAO dao = new ImageDAO();
    private final static int STEP = 1000;
    private final static int START = 0;
    private final static int END = 500000;

    public static Multiset<String> NGRAM_FREQUENCIES = ConcurrentHashMultiset.create();

    public static void main(String[] args) throws Exception {
       createNgramFile();
    }

    ///////////////////////////////////////////////////////
    //////////////// NGRAM FILE SECTION   /////////////////
    ///////////////////////////////////////////////////////

    private static void createNgramFile() throws IOException{
        MorphiaManager.setup("commoncrawl");
        NGramAnalyzer a = new NGramAnalyzer();
        a.extractNgramsFromCommonCrawl();
        MorphiaManager.tearDown();

        Iterable<Multiset.Entry<String>> multiset =
                Multisets.copyHighestCountFirst(NGRAM_FREQUENCIES).entrySet();
        PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter("/home/kandreadou/Documents/ngrams.txt",false)));
        for (Multiset.Entry<String> s : multiset) {
            if(s.getCount()<50)
                break;
            writer.println(s.getElement() + " " + s.getCount());
            //System.out.println(s.getElement() + " " + s.getCount());
        }
        writer.close();
    }

    private void extractNgramsFromCommonCrawl() {
        for (int k = START; k < END; k += STEP) {
            List<CrawledImage> list = dao.findRange(k, STEP);
            for (CrawledImage i : list) {
                processURL(i.normalizedSrc);
            }
        }
    }

    public static void processURL(String s) {
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
                extractNGrams(i);
            } catch (IOException ioe) {
                System.out.println(ioe);
            }
        }
        //System.out.println();
    }

    /**
     * Extracts ngrams from the given input string
     *
     * @param input
     * @throws IOException
     */
    public static void extractNGrams(String input) throws IOException {
        Reader reader = new StringReader(input);
        NGramTokenizer gramTokenizer = new NGramTokenizer(reader, MIN_NGRAM_SIZE, MAX_NGRAM_SIZE);
        gramTokenizer.reset();
        CharTermAttribute charTermAttribute = gramTokenizer.addAttribute(CharTermAttribute.class);

        while (gramTokenizer.incrementToken()) {
            String token = charTermAttribute.toString();
            NGRAM_FREQUENCIES.add(token);
            //System.out.print(token + ',');
        }

        gramTokenizer.end();
        gramTokenizer.close();
        reader.close();
    }
}
