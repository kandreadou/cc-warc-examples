package org.commoncrawl.mklab.ngrams;

import com.google.common.base.Strings;
import org.apache.lucene.analysis.ngram.NGramTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.commoncrawl.mklab.analysis.CrawledImage;
import org.commoncrawl.mklab.analysis.FilenameAnalyzer;
import org.commoncrawl.mklab.analysis.ImageDAO;
import org.commoncrawl.mklab.analysis.MorphiaManager;

import java.awt.*;
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
public class NGramArffCreator {

    private final static int STEP = 10000;
    private final static int START = 0;
    private final static int END = 500001;
    private final static int NUM_NGRAMS = 1000;
    private final static String[] NGRAMS = new String[NUM_NGRAMS];
    private final static String NGRAMS_FILE = "/home/kandreadou/Documents/commoncrawlstuff/ngrams_alt/ngramsalt.txt";
    private final static String ARFF_FILE = "/home/kandreadou/Documents/commoncrawlstuff/independent_testing/url.arff";

    public static void main(String[] args) throws Exception {
        NGramArffCreator n = new NGramArffCreator();
        MorphiaManager.setup("commoncrawl2");
        n.readNgramsFromFile();
        n.createArffFile();
        MorphiaManager.tearDown();

    }

    private boolean isBig2(CrawledImage i) throws IOException {

        Dimension d = FilenameAnalyzer.readFromFilewithImageReader(new File(FilenameAnalyzer.DOWNLOAD_FOLDER2 + i.filename));
        return d.getWidth() > 400 && d.getHeight() > 400;
    }

    private boolean isBig(CrawledImage i) throws IOException {
        //String imageName = "http://1.bp.blogspot.com/-0VJWooYsJHg/TbrdBv-FTAI/AAAAAAAACEM/drbz1_drAr8/s320/jf+jonah+peacock.bmp";
        //String imName = FilenameAnalyzer.getImageName(imageName);
        String imName = FilenameAnalyzer.getImageName(i.normalizedSrc);
        String suffix = FilenameAnalyzer.getSuffix(imName);
        if ("jpg".equalsIgnoreCase(suffix) || (!"png".equalsIgnoreCase(suffix) && !"bmp".equalsIgnoreCase(suffix) && !"tiff".equalsIgnoreCase(suffix) && !"gif".equalsIgnoreCase(suffix)))
            suffix = "jpeg";
        Dimension d;
        try {
            d = FilenameAnalyzer.readFromFilewithImageReader(new File(FilenameAnalyzer.DOWNLOAD_FOLDER + i.id + "." + suffix));
        } catch (FileNotFoundException fnf) {
            try {
                d = FilenameAnalyzer.readFromFilewithImageReader(new File(FilenameAnalyzer.DOWNLOAD_FOLDER + i.id + ".jpeg"));
            } catch (FileNotFoundException f) {
                try {
                    d = FilenameAnalyzer.readFromFilewithImageReader(new File(FilenameAnalyzer.DOWNLOAD_FOLDER + i.id + ".png"));
                } catch (FileNotFoundException f1) {
                    d = FilenameAnalyzer.readFromFilewithImageReader(new File(FilenameAnalyzer.DOWNLOAD_FOLDER + i.id + ".gif"));
                }
            }
        }
        return d.getWidth() > 400 && d.getHeight() > 400;
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

    private void createArffFile() throws IOException {
        ImageDAO dao = new ImageDAO();
        File arffFile = new File(ARFF_FILE);
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
            System.out.println("K= " + k);
            List<CrawledImage> list = dao.findRange(k, STEP);
            for (CrawledImage i : list) {
                try {
                    boolean[] ngramVector = getNGramVector(i);
                    boolean isBig = isBig2(i);
                    for (boolean b : ngramVector) {
                        bw.write(String.valueOf(b) + ',');
                    }
                    bw.write(isBig ? "big" : "small");
                    bw.newLine();
                } catch (IOException ioe) {
                    System.out.println(ioe);
                } catch (NullPointerException npe) {
                    //System.out.println(npe);
                }
            }
        }
        bw.close();
        fw.close();
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
