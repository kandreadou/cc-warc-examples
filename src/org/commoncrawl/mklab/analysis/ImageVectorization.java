package org.commoncrawl.mklab.analysis;

import georegression.struct.point.Point2D_F64;
import gr.iti.mklab.visual.aggregation.VladAggregatorMultipleVocabularies;
import gr.iti.mklab.visual.dimreduction.PCA;
import gr.iti.mklab.visual.extraction.AbstractFeatureExtractor;
import gr.iti.mklab.visual.extraction.ImageScaling;
import gr.iti.mklab.visual.extraction.SURFExtractor;
import gr.iti.mklab.visual.vectorization.ImageVectorizationResult;
import weka.classifiers.meta.CostSensitiveClassifier;
import weka.core.Attribute;
import weka.core.DenseInstance;
import weka.core.Instance;
import weka.core.Instances;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.Callable;

/**
 * Created by kandreadou on 2/17/15.
 */
public class ImageVectorization implements Callable<ImageVectorizationResult> {

    /**
     * Image will be scaled at this maximum number of pixels before vectorization.
     */
    private int maxImageSizeInPixels = 1024 * 768;

    /**
     * The filename of the image.
     */
    private String imageFilename;

    /**
     * The directory (full path) where the image resides.
     */
    private String imageFolder;

    /**
     * The image as a BufferedImage object.
     */
    private BufferedImage image;

    /**
     * The target length of the extracted vector.
     */
    private int vectorLength;

    /**
     * This object is used for descriptor extraction.
     */
    private static AbstractFeatureExtractor featureExtractor;

    /**
     * This object is used for extracting VLAD vectors with multiple vocabulary aggregation.
     */
    private static VladAggregatorMultipleVocabularies vladAggregator;

    /**
     * This object is used for PCA projection and whitening.
     */
    private static PCA pcaProjector;

    private static CostSensitiveClassifier svm;

    /**
     * If set to true, debug output is displayed.
     */
    public boolean debug = false;

    public void setDebug(boolean debug) {
        this.debug = debug;
    }

    /**
     * This constructor is used when the image should be read into a BufferedImage object from the given
     * folder.
     *
     * @param imageFolder          The folder (full path) where the image resides
     * @param imageFilename        The filename of the image
     * @param vectorLength         The target length of the vector
     * @param maxImageSizeInPixels The maximum image size of in pixels. It the image is larger, it is first scaled down prior
     *                             to vectorization.
     */
    public ImageVectorization(String imageFolder, String imageFilename, int vectorLength,
                              int maxImageSizeInPixels) {
        this.imageFolder = imageFolder;
        this.imageFilename = imageFilename;
        this.vectorLength = vectorLength;
        this.maxImageSizeInPixels = maxImageSizeInPixels;
    }

    /**
     * This constructor is used when the image has been already read into a BufferedImage object.
     *
     * @param imageFilename        The filename of the image
     * @param image                A BufferedImage object of the image
     * @param vectorLength         The target length of the vector
     * @param maxImageSizeInPixels The maximum image size of in pixels. It the image is larger, it is first scaled down prior
     *                             to vectorization.
     */
    public ImageVectorization(String imageFilename, BufferedImage image, int vectorLength,
                              int maxImageSizeInPixels) {
        this.imageFilename = imageFilename;
        this.vectorLength = vectorLength;
        this.image = image;
        this.maxImageSizeInPixels = maxImageSizeInPixels;
    }

    @Override
    /**
     * Returns an ImageVectorizationResult object from where the image's vector and name can be
     * obtained.
     */
    public ImageVectorizationResult call() throws Exception {
        if (debug)
            System.out.println("Vectorization for image " + imageFilename + " started.");
        double[] imageVector = transformToVector();
        if (debug)
            System.out.println("Vectorization for image " + imageFilename + " completed.");
        return new ImageVectorizationResult(imageFilename, imageVector, null);
    }

    public static long SCALING = 0;
    public static long FEATURE_EXTRACTION = 0;
    public static long CLASSIFY = 0;
    public static long VALD_PCA = 0;
    public static long TOTAL_TIME = 0;
    public static long counter = 0;

    public double[] transformToVector() throws Exception {
        if (vectorLength > vladAggregator.getVectorLength() || vectorLength <= 0) {
            throw new Exception("Vector length should be between 1 and " + vladAggregator.getVectorLength());
        }
        // first the image is read if the image field is null
        if (image == null) {
            try { // first try reading with the default class
                //System.out.println("Image folder "+imageFolder+" image Filename "+imageFilename);
                image = ImageIO.read(new File(imageFolder + imageFilename));
            } catch (IllegalArgumentException e) {
                // this exception is probably thrown because of a greyscale jpeg image
                System.out.println("Exception: " + e.getMessage() + " | Image: " + imageFilename);
                return new double[0];
                // retry with the modified class
                //image = ImageIOGreyScale.read(new File(imageFolder + imageFilename));
            }
        }
        counter++;
        long start = System.currentTimeMillis();
        long now = System.currentTimeMillis();
        // next the image is scaled
        ImageScaling scale = new ImageScaling(maxImageSizeInPixels);
        image = scale.maxPixelsScaling(image);

        SCALING += System.currentTimeMillis() - now;
        now = System.currentTimeMillis();
        // next the local features are extracted
        double[][] features = featureExtractor.extractFeatures(image);
        FEATURE_EXTRACTION += System.currentTimeMillis() - now;
        now = System.currentTimeMillis();
        //////////////////////// AREA CLASSIFIER ////////////////////////////
        //loadClassifier();

        ArrayList<Attribute> attributes = new ArrayList<Attribute>();
        for (int i = 0; i < 64; i++) {
            attributes.add(new Attribute("surf" + i));
        }
        ArrayList fvClassVal = new ArrayList<String>(2);
        fvClassVal.add("OUT");
        fvClassVal.add("IN");
        Attribute classAttribute = new Attribute("class", fvClassVal);
        attributes.add(classAttribute);
        // predict instance class values
        Instances data = new Instances("Test dataset", attributes, features.length);
        data.setClassIndex(64);

        for (int i = 0, len = features.length; i < len; i++) {
            double[] descriptor = features[i];
            // add data to instance
            data.add(new DenseInstance(1.0, descriptor));
        }

        ArrayList<double[]> filtered = new ArrayList<double[]>(features.length);

        for (int i = 0, len = data.numInstances(); i < len; i++) {
            // perform prediction
            Instance inst = data.instance(i);
            double[] distribution = svm.distributionForInstance(inst);
            if (distribution[0] > 0.7 && distribution[1] < 0.3) {
                continue;
            } else {
                filtered.add(features[i]);
            }
            //double myValue = svm.classifyInstance(inst);
            // get the name of class value
            //String label = data.classAttribute().value((int)myValue);
            //if("IN".equals(label)){
            //   filtered.add(features[i]);
            //}
            //int realvalue = (int) inst.classValue();
            //System.out.println("Prediction for instance " + i + " value: " + myValue + " prediction " + label);

        }

        filtered.trimToSize();
        double[][] featuresFiltered = filtered.toArray(new double[filtered.size()][64]);

        CLASSIFY += System.currentTimeMillis() - now;
        now = System.currentTimeMillis();
        //System.out.println("initial length "+features.length+" final length "+featuresFiltered.length);

        // next the features are aggregated
        double[] vladVector = vladAggregator.aggregate(featuresFiltered);

        if (vladVector.length == vectorLength) {
            // no projection is needed
            VALD_PCA += System.currentTimeMillis() - now;
            TOTAL_TIME += System.currentTimeMillis() - start;
            return vladVector;
        } else {
            // pca projection is applied
            double[] projected = pcaProjector.sampleToEigenSpace(vladVector);
            VALD_PCA += System.currentTimeMillis() - now;
            TOTAL_TIME += System.currentTimeMillis() - start;
            return projected;
        }
    }

    /**
     * Sets the FeatureExtractor object that will be used.
     *
     * @param extractor
     */
    public static void setFeatureExtractor(AbstractFeatureExtractor extractor) {
        ImageVectorization.featureExtractor = extractor;
    }

    /**
     * Sets the VladAggregatorMultipleVocabularies object that will be used.
     *
     * @param vladAggregator
     */
    public static void setVladAggregator(VladAggregatorMultipleVocabularies vladAggregator) {
        ImageVectorization.vladAggregator = vladAggregator;
    }

    /**
     * Sets the PCA projection object that will be used.
     *
     * @param pcaProjector
     */
    public static void setPcaProjector(PCA pcaProjector) {
        ImageVectorization.pcaProjector = pcaProjector;
    }

    /**
     * Loads the cost sensitive classifier.
     */
    public static void loadClassifier() {
        try {
            svm = (CostSensitiveClassifier) weka.core.SerializationHelper.read("/home/kandreadou/Desktop/classifier_training/models/correctRF32trees30cost5folds.model");
        } catch (Exception ex) {
            System.out.println("Exception when loading classifier " + ex);
        }
    }

    /**
     * Example of a single image vectorization using this class.
     *
     * @param args
     * @throws Exception
     */
    public static void main(String args[]) throws Exception {

        String learningFiles = "/home/manosetro/git/multimedia-indexing/learning_files/";

        File imageFolder = new File("/disk1_data/Photos/AkisGenethlia");

        String[] codebookFiles = {
                learningFiles + "surf_l2_128c_0.csv",
                learningFiles + "surf_l2_128c_1.csv",
                learningFiles + "surf_l2_128c_2.csv",
                learningFiles + "surf_l2_128c_3.csv"};

        int[] numCentroids = {128, 128, 128, 128};

        String pcaFilename = learningFiles + "pca_surf_4x128_32768to1024.txt";
        int initialLength = numCentroids.length * numCentroids[0] * AbstractFeatureExtractor.SURFLength;
        int targetLength = 1024;

        System.out.println("Initial length : " + numCentroids.length + "x" +
                numCentroids[0] + "x" + AbstractFeatureExtractor.SURFLength + "=" + initialLength);

        if (targetLength < initialLength) {
            PCA pca = new PCA(targetLength, 1, initialLength, true);
            pca.loadPCAFromFile(pcaFilename);
            ImageVectorization.setPcaProjector(pca);
            System.out.println("PCA loaded! ");
        }


        long t = System.currentTimeMillis();
        for (String imagFilename : imageFolder.list()) {

            ImageVectorization imvec = new ImageVectorization(imageFolder.toString() + "/", imagFilename, targetLength, 512 * 384);
            ImageVectorization.setFeatureExtractor(new SURFExtractor());
            //ImageVectorization.setVladAggregator(new VladAggregatorMultipleVocabularies(codebookFiles,
            //numCentroids, AbstractFeatureExtractor.SURFLength));

            imvec.setDebug(false);

            ImageVectorizationResult imvr = imvec.call();
            double[] vector = imvr.getImageVector();
            String vectorStr = Arrays.toString(vector);

            System.out.println(imvr.getImageName() + " : " + vector.length);

        }

        t = System.currentTimeMillis() - t;
        System.out.println(t + " msecs to extract features from " + imageFolder.list().length + " images");

    }
}
