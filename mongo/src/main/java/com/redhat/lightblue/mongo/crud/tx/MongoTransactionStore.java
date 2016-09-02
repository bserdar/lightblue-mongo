/*
 Copyright 2013 Red Hat, Inc. and/or its affiliates.

 This file is part of lightblue.

 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.redhat.lightblue.mongo.crud;

import java.util.Date;
import java.util.List;
import java.util.ArrayList;

import com.mongodb.DBCollection;
import com.mongodb.BasicDBObject;
import com.mongodb.WriteResult;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import org.bson.types.ObjectId;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements transaction store using a mongodb
 * collection. It assumes that the collection is dedicated to
 * transaction objects only, that is, all documents in this collection
 * are transaction documents.
 *
 */
public class MongoTransactionStore implements TransactionStore {

    private final Logger LOGGER=LoggerFactory.getLogger(MongoTransactionStore.class);

    private final String ST_ACTIVE=TransactionState.active.toString();
    private final String ST_RB=TransactionState.rolling_back.toString();
    private final String ST_COMMITTING=TransactionState.committing.toString();
    private final String ST_EXPIRED=TransactionState.expired.toString();
    
    private final DBCollection collection;

    public MongoTransactionStore(DBCollection collection) {
        this.collection=collection;
    }

    /**
     * Starts a new transaction in active state, and returns its unique id
     */
    @Override
    public String startTransaction(long timeoutMS) {
        String id=new ObjectId().toString();
        LOGGER.debug("Starting transaction {} with timeout={}ms",id,timeoutMS);
        Date now=new Date();
        Date exp=new Date(now.getTime()+timeoutMS);
        BasicDBObject doc=new BasicDBObject().
            append("_id",id).
            append("start",now).
            append("state",ST_ACTIVE).
            append("exp",exp);
        try {
            WriteResult result=collection.insert(doc);
            LOGGER.debug("Transaction {} writeresult={}",id,result);
            if(result.getN()!=1)
                throw new CannotStartTransactionException();
        } catch (Exception e) {
            LOGGER.error("Cannot start transaction {}",id,e);
            throw new CannotStartTransactionException(e);
        }
        return id;
    }

    /**
     * Changes the state of an active or expired transaction to
     * rollingback. If the tranasction is not found, or not in active
     * or expired state, throws InvalidTransactionException
     */
    @Override
    public void startRollback(String tx) throws InvalidTransactionException {
        LOGGER.debug("Starting rollback of {}",tx);
        List<String> expired_or_active=new ArrayList<>(2);
        expired_or_active.add(ST_ACTIVE);
        expired_or_active.add(ST_EXPIRED);
        WriteResult wr=collection.update(new BasicDBObject("_id",tx).
                                         append("state",
                                                new BasicDBObject("$in",expired_or_active)),
                                         new BasicDBObject("$set",
                                                           new BasicDBObject("state",ST_RB).
                                                           append("rbstart",new Date())));
        if(wr.getN()!=1) {
            LOGGER.info("Cannot start rollback of {}",tx);
            throw new InvalidTransactionException(tx);
        }
        LOGGER.debug("Started rollback of {}",tx);
    }

    /**
     * Removes a transaction that is in rollingback state
     */
    @Override
    public void endRollback(String tx) {
        LOGGER.debug("Ending rollback of {}",tx);
        collection.remove(new BasicDBObject("_id",tx).
                          append("state",ST_RB));
    }
    
    /**
     * Changes the state of an unexpired active transaction to
     * committing. Throws InvalidTransactionException if the
     * transaction is not found, or if it is expired, or in another
     * state.
     */
    @Override
    public void startCommit(String tx) throws InvalidTransactionException {
        LOGGER.debug("Starting commit of {}",tx);
        Date now=new Date();
        WriteResult wr=collection.update(new BasicDBObject("_id",tx).
                                         append("state",ST_ACTIVE).
                                         append("exp",new BasicDBObject("$gte",now)),
                                         new BasicDBObject("$set",
                                                           new BasicDBObject("state",ST_COMMITTING).
                                                           append("commitstart",now)));
        if(wr.getN()!=1) {
            LOGGER.info("Cannot start committing {}",tx);
            checkExpired(tx);
            throw new InvalidTransactionException(tx);
        }
        LOGGER.debug("Started committing {}",tx);
    }

    /**
     * Removes a transaction that is in committing state
     */
    @Override
    public void endCommit(String tx) {
        LOGGER.debug("Ending commit of {}",tx);
        collection.remove(new BasicDBObject("_id",tx).
                          append("state",ST_COMMITTING));
    }

    /**
     * Returns the current state of a transaction. This should be
     * called before committing a transaction
     */
    @Override
    public TransactionState getState(String tx) throws InvalidTransactionException {
        checkExpired(tx);
        DBCursor cursor=collection.find(new BasicDBObject("_id",tx));
        if(cursor.hasNext()) {
            DBObject doc=cursor.next();
            cursor.close();
            return TransactionState.valueOf((String)doc.get("state"));
        }
        cursor.close();
        throw new InvalidTransactionException(tx);
    }                                            

    /**
     * Marks an active transaction as expired if the expiration date is passed.
     *
     * @return true if this call expired the transaction
     */
    private boolean checkExpired(String tx) {
        LOGGER.debug("Checking if {} needs to be expired",tx);
        boolean b=collection.update(new BasicDBObject("_id",tx).
                                    append("state",ST_ACTIVE).
                                    append("exp",new BasicDBObject("$lte",new Date())),
                                    new BasicDBObject("$set",
                                                      new BasicDBObject("state",ST_EXPIRED))).getN()==1;
        if(b)
            LOGGER.debug("{} is expired now",tx);
        return b;
    } 
}
