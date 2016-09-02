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

/**
 * Top level interface for transaction controllers. This interface
 * provides the means to create a transaction by giving it a unique
 * id, ending a transaction, and pinging it so prevent premature
 * cleanup.
 */
public interface TransactionController {

    /**
     * Starts a new transaction, and returns a unique identifier for it
     *
     * @param timeoutMS if not null, this is the timeout value, otherwise default timeout is used
     */
    String startTransaction(Long timeoutMS);

    /**
     * Ends the transaction
     *
     * @param tx The transaction id
     *
     * @throws InvalidTransactionException
     */
    void endTransaction(String tx) throws InvalidTransactionException;

    /**
     * Touches the transaction to notify that the transaction is still
     * active. If the transaction is terminated already, throws the
     * InvalidTransactionException

     * @throws InvalidTransactionException
     */
    void touch(String tx) throws InvalidTransactionException;
}
