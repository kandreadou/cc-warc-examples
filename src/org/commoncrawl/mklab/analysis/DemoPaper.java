package org.commoncrawl.mklab.analysis;

import gr.iti.mklab.simmo.items.Image;
import gr.iti.mklab.simmo.morphia.MediaDAO;
import org.bson.types.ObjectId;

import java.awt.image.BufferedImage;
import java.io.*;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.zip.GZIPInputStream;

/**
 * Created by kandreadou on 2/18/15.
 */
public class DemoPaper {

    private MediaDAO<Image> imageDAO;
    private Set<String> y = new HashSet<String>();
    private int counter = 0;

    public DemoPaper() {
        IndexingManage.getInstance();
        imageDAO = new MediaDAO<Image>(Image.class, "demopaper5K");

    }

    private void doStuff() {
        for (String imgurl : y) {
            try {
                BufferedImage input = ImageUtils.downloadImage(imgurl.toString());
                if (input == null || input.getWidth() < 400 || input.getHeight() < 400) {
                    //do nothing
                } else if (IndexingManage.getInstance().indexImage(imgurl.toString(), input)) {
                    Image img = new Image();
                    img.setId(new ObjectId().toString());
                    img.setHeight(input.getWidth());
                    img.setWidth(input.getWidth());
                    imageDAO.save(img);
                }
            } catch (Exception e) {
                //System.out.println(e);
            }
        }
    }

    private void readFromZipFile(File file) throws IOException {
        if (file.getPath().endsWith(".gz")) {
            //processFile(file);
            BufferedReader reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(file)), "UTF-8"));
            String line = reader.readLine();
            while (line != null && y.size() < 8000) {
                counter++;
                String[] elements = line.split("\\s+");
                y.add(elements[0]);
                line = reader.readLine();
            }
            reader.close();
            reader = null;
        }
    }

    public static void main(String[] args) throws Exception {
        gr.iti.mklab.simmo.morphia.MorphiaManager.setup("127.0.0.1");
        DemoPaper p = new DemoPaper();
        p.readFromZipFile(new File("/home/kandreadou/Pictures/part-r-00000.gz"));
        long start = System.currentTimeMillis();
        p.doStuff();
        System.out.println("Counter " + ImageVectorization.counter);
        System.out.println("SCALING " + ImageVectorization.SCALING / ImageVectorization.counter);
        System.out.println("FEATURES " + ImageVectorization.FEATURE_EXTRACTION / ImageVectorization.counter);
        System.out.println("CLASSIFY " + ImageVectorization.CLASSIFY / ImageVectorization.counter);
        System.out.println("VLAD & PCA " + ImageVectorization.VALD_PCA / ImageVectorization.counter);
        System.out.println("INDEX " + IndexingManage.INDEXING / ImageVectorization.counter);
        System.out.print("Duration " + ((System.currentTimeMillis() - start) / 1000) + " seconds");
        System.out.println("Total pure time " + ImageVectorization.TOTAL_TIME / 1000);
        gr.iti.mklab.simmo.morphia.MorphiaManager.tearDown();
    }
}
