package org.commoncrawl.mklab.ngrams;

import com.google.common.base.Strings;
import org.apache.lucene.analysis.ngram.NGramTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.commoncrawl.mklab.analysis.CrawledImage;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by kandreadou on 1/5/15.
 */
public class NGramArffCreator extends IArffCreator {

    private final int NUM_NGRAMS;
    private final String[] NGRAMS;
    private final static String NGRAMS_FILE = "/home/kandreadou/Documents/commoncrawlstuff/ngrams_freq_url.txt";


    public NGramArffCreator(String filename, int numNgrams) throws IOException {
        super(filename);
        NUM_NGRAMS = numNgrams;
        NGRAMS = new String[NUM_NGRAMS];
    }

    @Override
    public void initialize() throws IOException {
        readNgramsFromFile();
        bw.write("@relation ngrams");
        bw.newLine();
        bw.newLine();
        for (int i = 0; i < NUM_NGRAMS; i++) {
            bw.write("@attribute ngram" + i + " numeric");
            bw.newLine();
        }
        bw.write("@attribute class {'SMALL','BIG'}");
        bw.newLine();
        bw.newLine();
        bw.write("@data");
        bw.newLine();
    }

    @Override
    public void writeFeatureVector(CrawledImage i, boolean isBig) throws IOException {
        boolean[] ngramVector = getNGramVector(i);

        for (boolean b : ngramVector) {
            bw.write(String.valueOf(b ? 1 : 0) + ',');
        }
        bw.write(isBig ? "BIG" : "SMALL");
        bw.newLine();
    }

    @Override
    public void teardown() throws IOException {
        bw.close();
        fw.close();
    }

    private void readNgramsFromFile() throws IOException {
        int index = 0;
        Path path = Paths.get(NGRAMS_FILE);
        List<String> lines = Files.readAllLines(path, Charset.forName("UTF-8"));
        for (String line : lines) {
            if (index >= NUM_NGRAMS)
                return;
            else {
                NGRAMS[index] = line.split("\\s+")[0];
                index++;
            }
        }
    }

    private boolean[] getNGramVectorAlt(CrawledImage i) throws NullPointerException, IOException {
        if (Strings.isNullOrEmpty(i.alt))
            throw new NullPointerException();
        boolean[] result = new boolean[NUM_NGRAMS];
        Set<String> ngrams = getNGramsFromStringAlt(i.alt);
        //System.out.println(ArrayUtils.toString(ngrams));
        for (int k = 0; k < NUM_NGRAMS; k++) {
            String ngramAtK = NGRAMS[k];
            result[k] = ngrams.contains(ngramAtK) ? true : false;
        }
        return result;
    }

    private boolean[] getNGramVector(CrawledImage i) {
        boolean[] result = new boolean[NUM_NGRAMS];
        Set<String> ngrams = getNGramsFromString(i.normalizedSrc);
        //System.out.println(ArrayUtils.toString(ngrams));
        for (int k = 0; k < NUM_NGRAMS; k++) {
            String ngramAtK = NGRAMS[k];
            result[k] = ngrams.contains(ngramAtK) ? true : false;
        }
        return result;
    }

    public Set<String> getNGramsFromStringAlt(String s) throws IOException {
        Set<String> ngrams = new HashSet<String>();

        try {
            Reader reader = new StringReader(s);
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

        return ngrams;
    }

    public Set<String> getNGramsFromString(String s) {
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
