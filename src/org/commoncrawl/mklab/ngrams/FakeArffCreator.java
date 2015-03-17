package org.commoncrawl.mklab.ngrams;

import org.apache.commons.lang.StringUtils;
import org.commoncrawl.mklab.analysis.CrawledImage;
import weka.classifiers.trees.RandomForest;
import weka.core.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by kandreadou on 3/17/15.
 */
public class FakeArffCreator extends IArffCreator {

    private RandomForest featuresClass;
    private RandomForest ngramsClass;
    private ScoreNgramArffCreator ngramsExtractor;


    @Override
    public void initialize() throws IOException {
        try {
            FileInputStream fis = new FileInputStream("/home/kandreadou/Documents/commoncrawlstuff/models/tfidf_2000_100trees.model");
            ObjectInputStream ois = new ObjectInputStream(fis);
            ngramsClass = (RandomForest) ois.readObject();
            ois.close();
            fis = new FileInputStream("/home/kandreadou/Documents/commoncrawlstuff/models/features_30trees.model");
            ois = new ObjectInputStream(fis);
            featuresClass = (RandomForest) ois.readObject();
            ois.close();
            ngramsExtractor = new ScoreNgramArffCreator("/home/kandreadou/Documents/commoncrawlstuff/junk.txt", 2000, "/home/kandreadou/Documents/commoncrawlstuff/ngrams_tfidf_2000.txt");
            ngramsExtractor.readNgramsFromFile();
        } catch (ClassNotFoundException cnf) {
            System.out.println(cnf);
        }
    }

    private List<CrawledImage> list = new ArrayList<CrawledImage>();

    @Override
    public void writeFeatureVector(CrawledImage image, boolean isBig) throws IOException {
        list.add(image);
    }

    @Override
    public void teardown() throws IOException {
        try {
            int TPbig = 0;
            int FPbig = 0;
            int FNbig = 0;
            int TPsmall = 0;
            int FPsmall = 0;
            int FNsmall = 0;
            Instances ngrams = createNGramData(list);
            Instances features = createFeatureData(list);
            for (int i = 0, len = ngrams.numInstances(); i < len; i++) {
                boolean isBig = false;
                // perform prediction
                Instance ngram = ngrams.instance(i);
                Instance feature = features.instance(i);

                double c1 = ngramsClass.classifyInstance(ngram);
                //isBig = c1==1;
                double c2 = featuresClass.classifyInstance(feature);
                //The classifiers agree
                if (c1 == c2) {
                    isBig = c1 == 1;
                    System.out.println("The classifiers agree " + c1);
                    System.out.println("Ground truth: isBig " + list.get(i).isBig + " isSmall " + list.get(i).isSmall);
                } else {
                    double[] d1 = ngramsClass.distributionForInstance(ngram);
                    System.out.println("d1 " + d1[0] + " " + d1[1]);
                    double[] d2 = featuresClass.distributionForInstance(feature);
                    System.out.println("d2 " + d2[0] + " " + d2[1]);
                    if (Math.abs(d1[0] - d1[1]) + 0.15 > Math.abs(d2[0] - d2[1]) )
                        isBig = c1 == 1;
                    else
                        isBig = c2 == 1;
                }
                boolean isReallyBig = list.get(i).isBig;
                System.out.println("Classifiers isBig " + isBig + " ground truth " + isReallyBig);
                if (isReallyBig) {
                    if (isBig)
                        TPbig++;
                    else {
                        FNbig++;
                        FPsmall++;
                    }
                }else{
                    if (!isBig)
                        TPsmall++;
                    else {
                        FNsmall++;
                        FPbig++;
                    }
                }
                System.out.println("Big TP "+TPbig+" FN "+FNbig+" "+"FP "+FPbig);
                System.out.println("Small TP "+TPsmall+" FN "+FNsmall+" "+"FP "+FPsmall);


            }
            double f1big = 2*TPbig/(double)(2*TPbig+FNbig+FPbig);
            double f1small = 2*TPsmall/(double)(2*TPsmall+FNsmall+FPsmall);
            double f1avg = (f1big+f1small)/(double)2;
            System.out.println("F1 score big "+f1big);
            System.out.println("F1 score small "+f1small);
            System.out.println("F1 average "+f1avg);

        } catch (Exception ex) {
            System.out.println(ex);
        }
    }

    private Instances createFeatureData(List<CrawledImage> list) {
        ArrayList<Attribute> attributes = new ArrayList<Attribute>();
        attributes.add(new Attribute("suffix_JEPG"));
        attributes.add(new Attribute("suffix_PNG"));
        attributes.add(new Attribute("suffix_BMP"));
        attributes.add(new Attribute("suffix_GIF"));
        attributes.add(new Attribute("suffix_TIFF"));
        attributes.add(new Attribute("domDepth"));
        attributes.add(new Attribute("domSiblings"));
        attributes.add(new Attribute("hasWidth"));
        attributes.add(new Attribute("width"));
        attributes.add(new Attribute("hasHeight"));
        attributes.add(new Attribute("height"));
        attributes.add(new Attribute("samedomain"));
        attributes.add(new Attribute("domElement_IMG"));
        attributes.add(new Attribute("domElement_LINK"));
        attributes.add(new Attribute("domElement_A"));
        attributes.add(new Attribute("domElement_EMBED"));
        attributes.add(new Attribute("domElement_IFRAME"));
        attributes.add(new Attribute("domElement_OBJECT"));
        attributes.add(new Attribute("hasAltText"));
        attributes.add(new Attribute("altTextLength"));
        attributes.add(new Attribute("hasParentText"));
        attributes.add(new Attribute("parentTextLength"));
        attributes.add(new Attribute("urlLength"));
        ArrayList fvClassVal = new ArrayList<String>(2);
        fvClassVal.add("SMALL");
        fvClassVal.add("BIG");
        Attribute classAttribute = new Attribute("class", fvClassVal);
        attributes.add(classAttribute);
        Instances data = new Instances("New instance", attributes, list.size());
        data.setClassIndex(23);
        for (CrawledImage i : list) {
            try {
                int index = 0;
                double[] featureVector = new double[23];
                String imUrl = i.normalizedSrc;
                //System.out.println(i.normalizedSrc);
                String imName = FeaturesArffCreator.getImageName(i.normalizedSrc);
                String suffix = FeaturesArffCreator.getSuffix(imName);
                //System.out.println(suffix);

                featureVector[index] = "jpeg".equals(suffix) ? 1 : 0;
                index++;
                featureVector[index] = "png".equals(suffix) ? 1 : 0;
                index++;
                featureVector[index] = "bmp".equals(suffix) ? 1 : 0;
                index++;
                featureVector[index] = "gif".equals(suffix) ? 1 : 0;
                index++;
                featureVector[index] = "tiff".equals(suffix) ? 1 : 0;
                index++;
                featureVector[index] = i.domDepth;
                index++;
                featureVector[index] = i.domSib;
                index++;


                //estimated dimensions from url
                int[] dims = FeaturesArffCreator.extractNumeric(i.normalizedSrc);
                featureVector[index] = dims[0] > 0 ? 1 : 0;
                index++;
                featureVector[index] = dims[0];
                index++;
                featureVector[index] = dims[1] > 0 ? 1 : 0;
                index++;
                featureVector[index] = dims[1];
                index++;

                try {
                    URL page = new URL(imUrl);
                    String imHost = page.getHost();
                    page = new URL(i.pageUrl);
                    String pageHost = page.getHost();
                    featureVector[index] = imHost.equalsIgnoreCase(pageHost) ? 1 : 0;
                    index++;
                } catch (MalformedURLException ex) {
                    System.out.println(ex);
                    featureVector[index] = 0;
                    index++;
                }

                featureVector[index] = "img".equals(i.domElem) ? 1 : 0;
                index++;
                featureVector[index] = "link".equals(i.domElem) ? 1 : 0;
                index++;
                featureVector[index] = "a".equals(i.domElem) ? 1 : 0;
                index++;
                featureVector[index] = "embed".equals(i.domElem) ? 1 : 0;
                index++;
                featureVector[index] = "iframe".equals(i.domElem) ? 1 : 0;
                index++;
                featureVector[index] = "object".equals(i.domElem) ? 1 : 0;
                index++;


                if (StringUtils.isEmpty(i.alt)) {
                    featureVector[index] = 0;
                    index++;
                    featureVector[index] = 0;
                    index++;
                } else {
                    featureVector[index] = 1;
                    index++;
                    featureVector[index] = i.alt.length();
                    index++;
                }
                if (StringUtils.isEmpty(i.parentTxt)) {
                    featureVector[index] = 0;
                    index++;
                    featureVector[index] = 0;
                    index++;
                } else {
                    featureVector[index] = 1;
                    index++;
                    featureVector[index] = i.parentTxt.length();
                    index++;
                }

                featureVector[index] = i.normalizedSrc.length();
                data.add(new DenseInstance(1, featureVector));

            } catch (NumberFormatException nfe) {

            }
        }
        return data;
    }

    private Instances createNGramData(List<CrawledImage> list) throws Exception {
        ArrayList<Attribute> attributes = new ArrayList<Attribute>();
        for (int i = 0; i < 2000; i++) {
            attributes.add(new Attribute("ngram" + i));
        }
        ArrayList fvClassVal = new ArrayList<String>(2);
        fvClassVal.add("SMALL");
        fvClassVal.add("BIG");
        Attribute classAttribute = new Attribute("class", fvClassVal);
        attributes.add(classAttribute);
        // predict instance class values
        Instances data = new Instances("New instance", attributes, list.size());
        data.setClassIndex(2000);

        for (CrawledImage image : list) {
            boolean[] ngramVector = ngramsExtractor.getNGramVector(image);
            double[] sparseVector = new double[ngramVector.length];
            for (int i = 0; i < ngramVector.length; i++)
                sparseVector[i] = ngramVector[i] ? 1 : 0;
            data.add(new SparseInstance(1, sparseVector));
        }
        return data;
    }
}
