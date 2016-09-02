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

import com.mongodb.DBCollection;
import com.mongodb.BasicDBObject;
import com.mongodb.WriteResult;

import org.bson.types.ObjectId;

/**
 * This class implements transaction management using a mongodb
 * collection. It assumes that the collection is dedicated to
 * transaction objects only, that is, all documents in this collection
 * are transaction documents.
 */
public class MongoTransactionController implements TransactionController {

    public enum State {active,rolling_back,committing,expired,ended};
    
    private final DBCollection collection;
    private final long transactionTimeoutMS;

    public MongoTransactionController(DBCollection collection,long transactionTimeoutMS) {
        this.collection=collection;
        this.transactionTimeoutMS=transactionTimeoutMS;
    }

    @Override
    public String startTransaction(Long timeoutMS) {
        String id=new ObjectId().toString();
        Date now=new Date();
        Date exp=new Date(now.getTime()+(timeoutMS==null?transactionTimeoutMS:timeoutMS.longValue()));
        BasicDBObject doc=new BasicDBObject().
            append("_id",id).
            append("start",now).
            append("state",State.active.toString()).
            append("exp",exp);
        try {
            WriteResult result=collection.insert(doc);
            if(result.getN()!=1)
                throw new CannotStartTransactionException();
        } catch (Exception e) {
            throw new CannotStartTransactionException(e);
        }
        return id;
    }

    @Override
    public void endTransaction(String tx) throws InvalidTransactionException {
        DBObject query=new BasicDBObject("_id",tx);
        DBObject doc=collection.findAndModify(query,
                                              new BasicDBObject("$set",
                                                                new BasicDBObject("state",State.ended.toString())));
        if(doc==null)
            throw new InvalidTransactionException(tx);
        if(doc.get("state").equals(State.active.toString()) && new Date().after(((Date)doc.get("exp")))) {
            collection.update(query,new BasicDBObject("state",State.expired.toString()));
            throw new InvalidTransactionException(tx);
        }
    }

    @Override
    public void checkActive(String tx) throws InvalidTransactionException {
        // We want this to be as fast as possible. For most cases, it
        // will simply check the state and timeout value
        DBCursor cursor=collection.find(new BasicDBObject("_id",tx).
                                        append("state",State.active.toString()).
                                        append("exp",new BasicDBObject("$gt",now)));
        boolean hasDoc=cursor.hasNext();
        cursor.close();
        if(!hasDoc) {
            throw new InvalidTransactionException(tx);
        }
    }
}
