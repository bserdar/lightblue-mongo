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
package com.redhat.lightblue.mongo.crud.tx;

import java.util.Map;
import java.util.HashMap;
import java.util.Date;

import org.bson.types.ObjectId;

/**
 * Implements transaction store using a hash table instance in memory
 */
public class MemoryTransactionStore implements TransactionStore {

    private static final class Tx {
        final String id;
        final Date start;
        Date expire;
        TransactionState state;

        public Tx() {
            this.id=new ObjectId().toString();
            this.start=new Date();
        }
    }
    
    private final Map<String,Tx> transactions=new HashMap<>();

    @Override
    public synchronized String startTransaction(long timeoutMS) {
        Tx tx=new Tx();
        tx.expire=new Date(tx.start.getTime()+timeoutMS);
        transactions.put(tx.id,tx);
        return tx.id;
    }

    @Override
    public synchronized void startRollback(String txid) throws InvalidTransactionException {
        Tx tx=transactions.get(txid);
        if(tx!=null&& (tx.state==TransactionState.active||tx.state==TransactionState.expired)) {
            tx.state=TransactionState.rolling_back;
        } else {
            throw new InvalidTransactionException(txid);
        }
    }

    @Override
    public synchronized void endRollback(String txid) {
        Tx tx=transactions.get(txid);
        if(tx!=null&&tx.state==TransactionState.rolling_back)
            transactions.remove(txid);
    }

    @Override
    public synchronized void startCommit(String txid) throws InvalidTransactionException {
        Tx tx=transactions.get(txid);
        if(tx!=null&&tx.state==TransactionState.active) {
            tx.state=TransactionState.committing;
        } else {
            checkExpired(txid);
            throw new InvalidTransactionException(txid);
        }
    }

    @Override
    public synchronized void endCommit(String txid) {
        Tx tx=transactions.get(txid);
        if(tx!=null&&tx.state==TransactionState.committing) {
            transactions.remove(txid);
        }
    }

    @Override
    public synchronized TransactionState getState(String txid) throws InvalidTransactionException {
        checkExpired(txid);
        Tx tx=transactions.get(txid);
        if(tx!=null)
            return tx.state;
        throw new InvalidTransactionException(txid);
    }                                            

    private boolean checkExpired(String txid) {
        Tx tx=transactions.get(txid);
        if(tx!=null&&tx.state==TransactionState.active&&new Date().after(tx.expire)) {
            tx.state=TransactionState.expired;
            return true;
        }
        return false;
    }
}
