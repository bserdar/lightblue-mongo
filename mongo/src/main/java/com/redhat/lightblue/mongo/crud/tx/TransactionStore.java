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
 * Top level interface for transaction stores. This interface provides
 * the means to create a transaction, ending a transaction, and
 * getting transaction status.
 */
public interface TransactionStore {

    /**
     * Starts a new transaction with the given timeout value.
     *
     * @param timeoutMS Transaction timeout value, in milliseconds
     * 
     * @return Transaction id
     */
    String startTransaction(long timeoutMS);

    /**
     * Changes the state of an active or expired transaction to
     * rollingback. If the transaction is not found, or not in active
     * or expired state, throws InvalidTransactionException
     */
    void startRollback(String tx) throws InvalidTransactionException;

    /**
     * Removes a transaction that is in rollingback state
     */
    void endRollback(String tx);
    
    /**
     * Changes the state of an unexpired active transaction to
     * committing. Throws InvalidTransactionException if the
     * transaction is not found, or if it is expired, or in another
     * state.
     */
    void startCommit(String tx) throws InvalidTransactionException;

    /**
     * Removes a transaction that is in committing state
     */
    void endCommit(String tx);

    /**
     * Returns the current state of a transaction. This should be
     * called before committing a transaction
     */
    TransactionState getState(String tx) throws InvalidTransactionException;
}
