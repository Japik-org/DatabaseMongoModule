package com.pro100kryto.server.modules;

import com.mongodb.MongoException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import com.pro100kryto.server.StartStopStatus;
import com.pro100kryto.server.logger.ILogger;
import com.pro100kryto.server.module.AModuleConnection;
import com.pro100kryto.server.module.Module;
import com.pro100kryto.server.modules.databasemongo.connection.IDatabaseMongoModuleConnection;
import com.pro100kryto.server.service.IServiceControl;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.jetbrains.annotations.Nullable;

public class DatabaseMongoModule extends Module {
    private MongoClient mongoClient = null;
    private MongoDatabase database = null;

    public DatabaseMongoModule(IServiceControl service, String name) {
        super(service, name);
    }

    @Override
    protected void startAction() throws Throwable {
        if (settings.containsKey("db-conn")){
            mongoClient = MongoClients.create(settings.get("db-conn"));
        } else {
            mongoClient = MongoClients.create();
        }

        String dbName = settings.getOrDefault("db-name", "");
        if (dbName.isEmpty()) throw new Throwable("db-name not specified");
        database = mongoClient.getDatabase(dbName);

        if (moduleConnection == null) {
            moduleConnection = new DatabaseMongoModuleConnection(logger, name, type);
        }
    }

    @Override
    protected void stopAction(boolean force) throws Throwable {
        mongoClient.close();
    }

    @Override
    public void tick() throws Throwable {
    }

    private final class DatabaseMongoModuleConnection extends AModuleConnection
            implements IDatabaseMongoModuleConnection {

        public DatabaseMongoModuleConnection(ILogger logger, String moduleName, String moduleType) {
            super(logger, moduleName, moduleType);
        }

        @Override
        public boolean isAliveModule() {
            return getStatus() == StartStopStatus.STARTED;
        }

        @Override
        public long estimatedCountDocuments(String collectionName){
            try {
                return database.getCollection(collectionName).estimatedDocumentCount();
            } catch (Throwable ignored){
            }
            return -1L;
        }

        @Override
        public long countDocuments(String collectionName){
            try {
                return database.getCollection(collectionName).countDocuments();
            } catch (Throwable ignored){
            }
            return -1L;
        }

        @Override
        public long countDocuments(String collectionName, Bson filter){
            try {
                return database.getCollection(collectionName).countDocuments(filter);
            } catch (Throwable ignored){
            }
            return -1L;
        }

        @Override @Nullable
        public Document findFirstDocument(String collectionName){
            try {
                return database.getCollection(collectionName).find().first();
            } catch (Throwable ignored){
            }
            return null;
        }

        @Override @Nullable
        public Document findFirstDocument(String collectionName, Bson filter){
            try {
                return database.getCollection(collectionName).find(filter).first();
            } catch (Throwable ignored){
            }
            return null;
        }

        @Override
        public boolean existCollection(String collectionName){
            try {
                database.getCollection(collectionName);
                return true;
            } catch (IllegalArgumentException ignored){
            }
            return false;
        }

        @Override
        public void createCollection(String collectionName){
            database.createCollection(collectionName);
        }

        @Override
        public FindIterable<Document> findDocuments(String collectionName) throws IllegalArgumentException{
            return database.getCollection(collectionName).find();
        }

        @Override
        public FindIterable<Document> findDocuments(String collectionName, Bson filter) throws IllegalArgumentException{
            return database.getCollection(collectionName).find(filter);
        }

        @Override
        public InsertOneResult insertDocument(String collectionName, Document document) throws MongoException {
            try {
                try {
                    return database.getCollection(collectionName).insertOne(document);
                } catch (IllegalArgumentException ignored){
                    database.createCollection(collectionName);
                    return database.getCollection(collectionName).insertOne(document);
                }
            } catch (Throwable ignored){
            }

            return InsertOneResult.unacknowledged();
        }

        @Override
        public UpdateResult updateDocument(String collectionName, Bson filter, Bson update) throws MongoException, IllegalArgumentException {
            return database.getCollection(collectionName).updateOne(filter, update);
        }

        @Override
        public UpdateResult replaceDocument(String collectionName, Bson filter, Document document) throws MongoException, IllegalArgumentException {
            return database.getCollection(collectionName).replaceOne(filter, document);
        }
    }
}
