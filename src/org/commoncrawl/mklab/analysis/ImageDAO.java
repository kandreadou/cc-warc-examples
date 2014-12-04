package org.commoncrawl.mklab.analysis;

import org.bson.types.ObjectId;
import org.mongodb.morphia.dao.BasicDAO;
import org.mongodb.morphia.query.Query;

import java.util.List;


/**
 * Created by kandreadou on 11/18/14.
 */
public class ImageDAO extends BasicDAO<CrawledImage, ObjectId> {

    public ImageDAO() {
        super(CrawledImage.class, MorphiaManager.getMongoClient(), MorphiaManager.getMorphia(), MorphiaManager.getDB().getName());
    }

    public List<CrawledImage> findRange(int start, int num){
        return MorphiaManager.getDatastore().find(CrawledImage.class).offset(start).limit(num).asList();
    }
}
