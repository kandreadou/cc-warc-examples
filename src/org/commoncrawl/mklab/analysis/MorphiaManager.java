package org.commoncrawl.mklab.analysis;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.Morphia;

/**
 * Created by kandreadou on 11/18/14.
 */
public class MorphiaManager {

    private static MongoClient mongoClient;
    private static DB db;
    private static Datastore ds;
    private static final Morphia morphia = new Morphia();

    private MorphiaManager(String dbName) {
        try {
            mongoClient = new MongoClient(new MongoClientURI("mongodb://localhost:27017"));
            db = mongoClient.getDB(dbName);
            ds = morphia.createDatastore(mongoClient, db.getName());
            morphia.map(CrawledImage.class);
            ds.ensureCaps();
            ds.ensureIndexes();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void setup(String dbName) {
        new MorphiaManager(dbName);
    }

    public static void tearDown() {
        mongoClient.close();
    }

    public static MongoClient getMongoClient() {
        if (mongoClient == null)
            throw new RuntimeException("MorphiaManager has not been properly initialized. Call setup");
        return mongoClient;
    }

    public static Morphia getMorphia() {
        if (morphia == null)
            throw new RuntimeException("MorphiaManager has not been properly initialized. Call setup");
        return morphia;
    }

    public static DB getDB() {
        if (db == null)
            throw new RuntimeException("MorphiaManager has not been properly initialized. Call setup");
        return db;
    }

    public static Datastore getDatastore(){
        if (ds == null)
            throw new RuntimeException("MorphiaManager has not been properly initialized. Call setup");
        return ds;
    }
}
