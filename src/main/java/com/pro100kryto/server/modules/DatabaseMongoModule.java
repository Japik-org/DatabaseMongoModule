package com.pro100kryto.server.modules;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.pro100kryto.server.StartStopStatus;
import com.pro100kryto.server.logger.ILogger;
import com.pro100kryto.server.module.AModuleConnection;
import com.pro100kryto.server.module.Module;
import com.pro100kryto.server.modules.databasemongo.connection.IDatabaseMongoModuleConnection;
import com.pro100kryto.server.service.IServiceControl;
import com.sun.istack.Nullable;

public class DatabaseMongoModule extends Module {
    private MongoClient mongoClient = null;

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
        Thread.yield();
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

        @Override @Nullable
        public MongoDatabase getMongoDatabase(final String dbName){
            try {
                return mongoClient.getDatabase(dbName);
            } catch (Throwable ignored){
            }
            return null;
        }

        @Override
        public MongoIterable<String> getListDatabaseNames(){
            return mongoClient.listDatabaseNames();
        }
    }
}
