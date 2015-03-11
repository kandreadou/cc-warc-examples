package org.commoncrawl.mklab.ngrams;

import weka.classifiers.trees.RandomForest;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.converters.ArffLoader;
import weka.core.converters.ConverterUtils;

import java.io.*;
import java.util.zip.GZIPInputStream;

/**
 * Created by kandreadou on 3/10/15.
 */
public class WekaTrainer {

    public static void main(String[] args) throws Exception{
        WekaTrainer t = new WekaTrainer();
        t.train();
    }

    public void train() throws Exception{
        System.out.println("Starting");
        //ConverterUtils.DataSource source = new ConverterUtils.DataSource("/home/kandreadou/Documents/commoncrawlstuff/ngrams_url_train_2000_sparse.arff");
        ConverterUtils.DataSource source = new ConverterUtils.DataSource("/home/iti-310/classification/ngrams_url_train_2000_sparse.arff");
        Instances structure = source.getDataSet();
        //ArffLoader loader = new ArffLoader();
        //loader.setFile(new File("/home/kandreadou/Documents/commoncrawlstuff/features_train.arff"));
        //Instances structure = loader.getStructure();
        structure.setClassIndex(structure.numAttributes() - 1);
        System.out.println("Structure loaded");
        RandomForest rf = new RandomForest();
        rf.setMaxDepth(0);
        rf.setNumExecutionSlots(8);
        rf.setNumFeatures(0);
        rf.setNumTrees(30);
        rf.setPrintTrees(false);
        rf.setSeed(1);
        rf.setDebug(false);
        System.out.println("Before build");
        rf.buildClassifier(structure);
        System.out.println("After build");
        //weka.core.SerializationHelper.write("/home/kandreadou/Documents/commoncrawlstuff/models/grams2000_10trees.model", rf);
        weka.core.SerializationHelper.write("/home/iti-310/classification/grams2000_30trees.model", rf);
    }

    public void createSparse() throws Exception{

        boolean startOfDataFound = false;
        File arffFile = new File("/home/kandreadou/Documents/commoncrawlstuff/ngrams_url_train_2000_sparse.arff");
        FileWriter fw = new FileWriter(arffFile.getAbsoluteFile());
        BufferedWriter bw = new BufferedWriter(fw);

        BufferedReader reader = new BufferedReader(new InputStreamReader((new FileInputStream("/home/kandreadou/Documents/commoncrawlstuff/ngrams_url_train_2000.arff")), "UTF-8"));
        String line = reader.readLine();
        while (line != null){
            if("@data".equals(line)){
                bw.write(line);
                startOfDataFound = true;
            }else {
                if (!startOfDataFound)
                    bw.write(line);
                else {
                    bw.write("{");
                    String[] elements = line.split(",");
                    if(elements.length<2000)
                        break;
                    for (int i = 0; i < 2001; i++) {
                        if ("1".equals(elements[i]))
                            bw.write(i + " " + 1 + ", ");
                    }
                    bw.write("2000 " + elements[2000] + "}");
                }
            }
            bw.newLine();
            line = reader.readLine();
        }
        reader.close();
        reader = null;
        bw.close();
        fw.close();
    }


}
