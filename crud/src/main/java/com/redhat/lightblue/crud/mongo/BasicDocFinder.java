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
package com.redhat.lightblue.crud.mongo;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.DBCursor;

import com.redhat.lightblue.interceptor.InterceptPoint;
import com.redhat.lightblue.crud.CRUDOperationContext;
import com.redhat.lightblue.crud.CRUDOperation;
import com.redhat.lightblue.crud.DocCtx;
import com.redhat.lightblue.mongo.hystrix.FindCommand;

import com.redhat.lightblue.util.JsonDoc;

/**
 * Basic doc search operation
 */
public class BasicDocFinder implements DocFinder {

    private static final Logger LOGGER = LoggerFactory.getLogger(BasicDocFinder.class);

    private final Translator translator;

    public BasicDocFinder(Translator translator) {
        this.translator = translator;
    }

    @Override
    public long find(CRUDOperationContext ctx,
                     DBCollection coll,
                     DBObject mongoQuery,
                     DBObject mongoProjection,
                     DBObject mongoSort,
                     Long from,
                     Long to) {
        LOGGER.debug("Submitting query {}",mongoQuery);
        DBCursor cursor = new FindCommand(coll, mongoQuery, mongoProjection).executeAndUnwrap();
        LOGGER.debug("Query evaluated");
        if (mongoSort != null) {
            cursor = cursor.sort(mongoSort);
            LOGGER.debug("Result set sorted");
        }
        int size = cursor.size();
        long ret = size;
        LOGGER.debug("Applying limits: {} - {}", from, to);
        if (from != null) {
            cursor.skip(from.intValue());
            if(to!=null && from>to)            
            	cursor.skip(size);	        
        }
        
        if (to != null) {
        	if(to >= 0)
            cursor.limit(to.intValue() - (from == null ? 0 : from.intValue()) + 1);
        	else if(to < 0)
        		cursor.skip(size);
        }
        
        
        LOGGER.debug("Retrieving results");
        List<DBObject> mongoResults = cursor.toArray();
        LOGGER.debug("Retrieved {} results", mongoResults.size());
        List<JsonDoc> jsonDocs = translator.toJson(mongoResults);
        ctx.addDocuments(jsonDocs);
        for (DocCtx doc : ctx.getDocuments()) {
            doc.setCRUDOperationPerformed(CRUDOperation.FIND);
            ctx.getFactory().getInterceptors().callInterceptors(InterceptPoint.POST_CRUD_FIND_DOC, ctx, doc);
        }
        LOGGER.debug("Translated DBObjects to json");
        return ret;
    }
}
