package org.commoncrawl.mklab.ngrams;

import org.commoncrawl.mklab.analysis.CrawledImage;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Created by kandreadou on 3/6/15.
 */
public abstract class IArffCreator {

    protected FileWriter fw;
    protected BufferedWriter bw;

    public IArffCreator(String filename) throws IOException {
        File arffFile = new File(filename);
        fw = new FileWriter(arffFile.getAbsoluteFile());
        bw = new BufferedWriter(fw);
        initialize();
    }

    public abstract void initialize() throws IOException;

    public abstract void writeFeatureVector(CrawledImage i, boolean isBig) throws IOException;

    public abstract void teardown() throws IOException;
}
