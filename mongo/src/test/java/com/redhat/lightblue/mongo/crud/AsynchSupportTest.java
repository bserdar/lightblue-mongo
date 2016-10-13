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

import java.util.ArrayList;
import java.util.List;
import java.util.HashSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.redhat.lightblue.ExecutionOptions;
import com.redhat.lightblue.AsynchRequest;
import com.redhat.lightblue.AsynchResponse;
import com.redhat.lightblue.Response;
import com.redhat.lightblue.AsynchStatus;
import com.redhat.lightblue.extensions.asynch.AsynchronousJob;
import com.redhat.lightblue.config.*;
import com.redhat.lightblue.crud.InsertionRequest;
import com.redhat.lightblue.metadata.ArrayField;
import com.redhat.lightblue.metadata.CompositeMetadata;
import com.redhat.lightblue.metadata.EntityMetadata;
import com.redhat.lightblue.metadata.FieldConstraint;
import com.redhat.lightblue.metadata.FieldCursor;
import com.redhat.lightblue.metadata.Index;
import com.redhat.lightblue.metadata.IndexSortKey;
import com.redhat.lightblue.metadata.MetadataStatus;
import com.redhat.lightblue.metadata.ObjectArrayElement;
import com.redhat.lightblue.metadata.ObjectField;
import com.redhat.lightblue.metadata.SimpleArrayElement;
import com.redhat.lightblue.metadata.SimpleField;
import com.redhat.lightblue.metadata.Version;
import com.redhat.lightblue.mongo.common.DBResolver;
import com.redhat.lightblue.mongo.common.MongoDataStore;
import com.redhat.lightblue.mongo.config.MongoConfiguration;
import com.redhat.lightblue.query.Projection;
import com.redhat.lightblue.util.Error;
import com.redhat.lightblue.util.JsonDoc;
import com.redhat.lightblue.util.Path;
import com.redhat.lightblue.util.JsonUtils;

public class AsynchSupportTest extends AbstractMongoCrudTest {

    private MongoCRUDController controller;

    @Override
    @Before
    public void setup() throws Exception {
        super.setup();

        DataSourcesConfiguration datasources=new DataSourcesConfiguration();
        datasources.initializeFromJson(JsonUtils.json(getClass().getResourceAsStream("/datasources.json"),true));        
        LightblueFactory lbFactory=new LightblueFactory(datasources);

        final DB dbx = db;
        // Cleanup stuff
        dbx.getCollection(COLL_NAME).drop();
        dbx.createCollection(COLL_NAME, null);

        controller = new MongoCRUDController(null, new DBResolver() {
            @Override
            public DB get(MongoDataStore store) {
                return dbx;
            }

            @Override
            public MongoConfiguration getConfiguration(MongoDataStore store) {
                return null;
            }
        });
        controller.setLightblueFactory(lbFactory);
    }

    @Test
    public void testSchedule() throws Exception {
        db.getCollection("asynch").remove(new BasicDBObject());
        InsertionRequest ireq=new InsertionRequest();
        ObjectNode doc=JsonNodeFactory.instance.objectNode();
        doc.set("name",JsonNodeFactory.instance.textNode("value"));
        ireq.setEntityData(doc);
        AsynchRequest req=new AsynchRequest(ireq);
        AsynchResponse response=controller.scheduleAsynchronousExecution(req);
        System.out.println("JobId:"+response.getJobId());
        Assert.assertEquals(AsynchStatus.scheduled,response.getAsynchStatus());
        Assert.assertNotNull(response.getScheduledTime());
        Assert.assertNull(response.getExecutionStartTime());
        Assert.assertNull(response.getCompletionTime());
        Assert.assertNull(response.getTimeoutTime());

        AsynchResponse response2=controller.getAsynchronousExecutionStatus(response.getJobId());
        Assert.assertEquals(response2.getJobId(),response.getJobId());
        Assert.assertEquals(response2.getAsynchStatus(),response.getAsynchStatus());
        Assert.assertEquals(response2.getScheduledTime(),response.getScheduledTime());
        Assert.assertNull(response2.getExecutionStartTime());
        Assert.assertNull(response2.getCompletionTime());
        Assert.assertNull(response2.getTimeoutTime());
        
    }

    @Test
    public void testGetAndLock() throws Exception {
        db.getCollection("asynch").remove(new BasicDBObject());
        for(int i=0;i<10;i++) {
                InsertionRequest ireq=new InsertionRequest();
                ObjectNode doc=JsonNodeFactory.instance.objectNode();
                doc.set("name",JsonNodeFactory.instance.textNode("value"));
                ireq.setEntityData(doc);
                AsynchRequest req=new AsynchRequest(ireq);
                req.setPriority(100-i);
                controller.scheduleAsynchronousExecution(req);
        }

        int n=0;
        AsynchronousJob job;
        HashSet<String> ids=new HashSet<>();
        int lastPriority=-1;
        while( (job=controller.getAndLockNextAsynchronousJob())!=null) {
            n++;

            Assert.assertTrue(ids.add(job.jobId));
            Assert.assertTrue(job.singleData.request instanceof InsertionRequest);
            System.out.println("Priority:"+job.priority);
            Assert.assertTrue(job.priority>lastPriority);
            lastPriority=job.priority;

            Assert.assertEquals(AsynchStatus.executing,job.asynchStatus);
        }

        Assert.assertEquals(10,n);
    }

    @Test
    public void testComplete() throws Exception {
        db.getCollection("asynch").remove(new BasicDBObject());
        InsertionRequest ireq=new InsertionRequest();
        ObjectNode doc=JsonNodeFactory.instance.objectNode();
        doc.set("name",JsonNodeFactory.instance.textNode("value"));
        ireq.setEntityData(doc);
        AsynchRequest req=new AsynchRequest(ireq);
        AsynchResponse aresponse=controller.scheduleAsynchronousExecution(req);

        AsynchronousJob job=controller.getAndLockNextAsynchronousJob();
        Assert.assertEquals(job.jobId,aresponse.getJobId());
        Assert.assertTrue(job.singleData.request instanceof InsertionRequest);
        Response response=new Response();
        ArrayNode data=JsonNodeFactory.instance.arrayNode();
        //Make it big
        for(int i=0;i<100000;i++) {
            ObjectNode node=JsonNodeFactory.instance.objectNode();
            node.set("value",JsonNodeFactory.instance.textNode("00000000000000000000000000000000000000000000000000000000000"));
            node.set("index",JsonNodeFactory.instance.numberNode(i));
            data.add(node);
        }
        response.setEntityData(data);
        job.singleData.response=response;
        controller.completeJob(job);

        AsynchResponse r=controller.getAsynchronousExecutionStatus(job.jobId);
        Assert.assertNotNull(r);
        Assert.assertEquals(AsynchStatus.completed,r.getAsynchStatus());
        ArrayNode retrievedData=(ArrayNode)r.getResponse().getEntityData();
        Assert.assertEquals(data.size(),retrievedData.size());
    }
}
