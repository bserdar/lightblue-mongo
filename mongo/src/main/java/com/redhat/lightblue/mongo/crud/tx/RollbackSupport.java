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

/**
 * Interface for rollback implementations.
 */
public interface RollbackSupport {

    /**
     * Called when a new document is inserted into a collection. The
     * implementation should store the document id so if the
     * transaction is rolled back, it can be removed
     */
    void logInsertion(String txid,String collectionName,String docid);

    /**
     * Called when a document is deleted from a collection. The
     * implementation should store a copy of the document so it can be
     * restored if the transaction is rolled back
     */
    void logDeletion(String txid,String collectionName,DBObject originalDoc);

    /**
     * Called when a document is updated. The implementation should
     * store the unmodified copy of the document so it can be restored
     * if the transaction is rolled back
     */
    void logUpdate(String txid,String collectionName,DBObject originalDoc);

    /**
     * Rolls back a transaction 
     */
    void rollback(String txid);

    /**
     * Clears the rollback log for a transaction
     */
    void commit(String txid);
}


