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

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;

/**
 * Sequence generation using a MongoDB collection.
 *
 * Each sequence is a document uniquely identified by the sequence name. The
 * document contains initial value for the sequence, the increment, and the
 * value.
 */
public class MongoSequenceGenerator {

    public static final String NAME = "name";
    public static final String INIT = "initialValue";
    public static final String INC = "increment";
    public static final String VALUE = "value";

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoSequenceGenerator.class);

    private final DBCollection coll;

    // a set of sequances collections which were already initialized
    private static Set<String> initializedCollections = new CopyOnWriteArraySet<>();

    public MongoSequenceGenerator(DBCollection coll) {
        this.coll = coll;

        if (!initializedCollections.contains(coll.getFullName())) {
            // Here, we also make sure we have the indexes setup properly
            initIndex();
            initializedCollections.add(coll.getFullName());
            LOGGER.info("Initialized sequances collection {}", coll.getFullName());
        }
    }

    private void initIndex() {
        // Make sure we have a unique index on name
        BasicDBObject keys = new BasicDBObject(NAME, 1);
        BasicDBObject options = new BasicDBObject("unique", 1);
        // ensureIndex was deprecated, changed to an alias of createIndex, and removed in a more recent version
        coll.createIndex(keys, options);
    }

    /**
     * Atomically increments and returns the sequence value. If this is the
     * first use of the sequence, the sequence is created
     *
     * @param name The sequence name
     * @param init The initial value of the sequence. Used only if the sequence
     * does not exists prior to this call
     * @param inc The increment, Could be negative or positive. If 0, it is
     * assumed to be 1. Used only if the sequence does not exist prior to this
     * call
     *
     * If the sequence already exists, the <code>init</code> and
     * <code>inc</code> are ignored.
     *
     * @return The value of the sequence before the call
     */
    public long getNextSequenceValue(String name, long init, long inc) {
        LOGGER.debug("getNextSequenceValue({})", name);
        // Read the sequence document
        BasicDBObject q = new BasicDBObject(NAME, name);
        DBObject doc = coll.findOne(q,null,ReadPreference.primary());
        if (doc == null) {
            // Sequence document does not exist. Insert a new document using the init and inc
            LOGGER.debug("inserting sequence record name={}, init={}, inc={}", name, init, inc);
            if (inc == 0) {
                inc = 1;
            }

            BasicDBObject u = new BasicDBObject().
                    append(NAME, name).
                    append(INIT, init).
                    append(INC, inc).
                    append(VALUE, init);
            try {
                coll.insert(u, WriteConcern.ACKNOWLEDGED);
            } catch (Exception e) {
                // Someone else might have inserted already, try to re-read
                LOGGER.debug("Insertion failed with {}, trying to read", e);
            }
            doc = coll.findOne(q,null,ReadPreference.primary());
            if (doc == null) {
                throw new RuntimeException("Cannot generate value for " + name);
            }
        }
        LOGGER.debug("Sequence doc={}", doc);
        Long increment = (Long) doc.get(INC);
        BasicDBObject u = new BasicDBObject().
                append("$inc", new BasicDBObject(VALUE, increment));
        // This call returns the unmodified document
        doc = coll.findAndModify(q, u);
        Long l = (Long) doc.get(VALUE);
        LOGGER.debug("{} -> {}", name, l);
        return l;
    }
}
