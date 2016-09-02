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

import java.util.Map;
import java.util.HashMap;

import org.bson.types.ObjectId;

public class MemoryTransactionController implements TransactionController {

    /**
     * Map of transaction id -> timestamp
     */
    private final Map<String,Long> transactions=new HashMap<>();
    
    @Override
    public synchronized String startTransaction() {
        String ret=new ObjectId().toString();
        transactions.put(ret,System.currentTimeMillis());
        return ret;
    }

    @Override
    public synchronized void endTransaction(String tx) throws InvalidTransactionException {
        if(transactions.remove(tx)==null)
            throw new InvalidTransactionException(tx);
    }

    @Override
    public synchronized void touch(String tx) throws InvalidTransactionException {
        if(transactions.put(tx,System.currentTimeMillis())==null)
            throw new InvalidTransactionException(tx);
    }
}
