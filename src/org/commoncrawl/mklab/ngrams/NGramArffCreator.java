package org.commoncrawl.mklab.ngrams;

import org.apache.lucene.analysis.ngram.NGramTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.commoncrawl.mklab.analysis.CrawledImage;
import org.commoncrawl.mklab.analysis.ImageDAO;
import org.commoncrawl.mklab.analysis.MorphiaManager;

import java.io.*;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by kandreadou on 1/5/15.
 */
public class NGramArffCreator {

    private final static int STEP = 1000;
    private final static int START = 0;
    private final static int END = 5000;
    private final static int NUM_NGRAMS = 2000;
    private final static String[] NGRAMS = new String[NUM_NGRAMS];

    public static void main(String[] args) throws Exception{
        MorphiaManager.setup("commoncrawl");
        NGramArffCreator n = new NGramArffCreator();
        n.createArffFile();
        MorphiaManager.tearDown();
    }

    private void createArffFile() throws IOException {
        //TODO: Load NGRAM_VECTOR
        NGramAnalyzer a = new NGramAnalyzer();
        ImageDAO dao = new ImageDAO();

        File arffFile = new File("/home/kandreadou/Documents/ngramtraining.arff");
        FileWriter fw = new FileWriter(arffFile.getAbsoluteFile());
        BufferedWriter bw = new BufferedWriter(fw);
        bw.write("@relation ngrams");
        bw.newLine();
        bw.newLine();
        for (int i = 0; i < NUM_NGRAMS; i++) {
            bw.write("@attribute ngram" + i + " {false, true}");
            bw.newLine();
        }
        bw.write("@attribute class {small, big}");
        bw.newLine();
        bw.newLine();
        bw.write("@data");
        bw.newLine();

        for (int k = START; k < END; k += STEP) {
            List<CrawledImage> list = dao.findRange(k, STEP);
            for (CrawledImage i : list) {
                try {
                    boolean[] ngramVector = getNGramVector(i);
                    for (boolean b : ngramVector) {
                        bw.write(String.valueOf(b) + ',');
                    }
                    //TODO: Check if big or small
                    if (true)
                        bw.write("small");
                    else
                        bw.write("big");
                    bw.newLine();
                } catch (IOException ioe) {
                    System.out.println(ioe);
                }
            }
        }
        bw.close();
        fw.close();
    }

    private boolean[] getNGramVector(CrawledImage i) {
        boolean[] result = new boolean[NUM_NGRAMS];
        Set<String> ngrams = getNGrams(i.normalizedSrc);
        //System.out.println(ArrayUtils.toString(ngrams));
        for (int k = 0; k < NUM_NGRAMS; k++) {
            String ngramAtK = NGRAMS[k];
            result[k] = ngrams.contains(ngramAtK) ? true : false;
        }
        return result;
    }

    public Set<String> getNGrams(String s) {
        Set<String> ngrams = new HashSet<String>();
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

        for (String i : items) {
            try {
                Reader reader = new StringReader(i);
                NGramTokenizer gramTokenizer = new NGramTokenizer(reader, NGramAnalyzer.MIN_NGRAM_SIZE, NGramAnalyzer.MAX_NGRAM_SIZE);
                gramTokenizer.reset();
                CharTermAttribute charTermAttribute = gramTokenizer.addAttribute(CharTermAttribute.class);

                while (gramTokenizer.incrementToken()) {
                    String token = charTermAttribute.toString();
                    ngrams.add(token);
                }

                gramTokenizer.end();
                gramTokenizer.close();
                reader.close();
            } catch (IOException ioe) {
                System.out.println(ioe);
            }
        }
        return ngrams;
    }

}
