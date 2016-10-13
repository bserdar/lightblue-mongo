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

import com.redhat.lightblue.AsynchRequest;
import com.redhat.lightblue.AsynchResponse;;

import com.redhat.lightblue.config.LightblueFactory;
import com.redhat.lightblue.config.LightblueFactoryAware;

import com.redhat.lightblue.extensions.asynch.AsynchronousExecutionSupport;
import com.redhat.lightblue.extensions.asynch.AsynchronousJob;

/**
 * This implementation of asynchronous execution support manages a job
 * store with three types of documents.
 * 
 *   job record: This is a small record with the job status information. 
 *       It has the status and priority fields. The _id is the job id.
 *   job request data: This contains the request data submitted with the job. 
 *         It has the requestOwner field, which is the jobId.
 *   job response data: This contains the response data, and there can be multiple. It has responseOwner, and 
 *         responseSequence fields. responseSequence determines the order of the response record in the
 *         overall response, if the response is large and broken into multiple pieces.
 */
public class MongoAsynchronousExecutionSupport implements AsynchronousExecutionSupport, LightblueFactoryAware {

    private LightblueFactory lbFactory;
    private AsynchronousExecutionConfiguration cfg;

    @Override
    public void setLightblueFactory(LightblueFactory factory) {
        this.lbFactory=factory;
        this.cfg=lbFactory.getFactory().getAsynchronousExecutionConfiguration();
    }

    @Override
    public AsynchResponse scheduleAsynchronousExecution(AsynchRequest request) {
        DBCollection coll=getJobStore();
        Date now=new Date();

        DBObject bsonJob=new BasicDBObject("createdTime",now).
            append("status",AsynchStatus.scheduled.toString()).
            append("scheduledTime",request.getExecuteAfter()==null?now:request.getExecuteAfter()).
            append("priority",request.getPriority());
        
        
        AsynchResponse response=new AsynchResponse();
        response.setPriority(request.getPriority());
        response.scheduledTime(now);
        response.setAsynchStatus(AsynchStatus.scheduled);
        
        DBObject bsonJob=jobToBson(request);
        bsonJob.set("scheduledTime",now);
        bsonJob.set("asynchStatus",AsynchStatus.scheduled.toString());
        DBObject bsonData=requestDataToBson(request);
        try { 
            coll.insert(bsonJon);
            bsonData.set("jobId",bsonJob.get("_id"));
            coll.insert(bsonData);
        } catch (Exception e) {
            throw Error.get(MongoCrudConstants.ERR_ASYNCH_SCHEDULING,e.toString());
        }
        response.setJobId(bsonRequest.get("_id"));
        return response;
    }

    @Override
    public AsynchResponse getAsynchronousExecutionStatus(String jobId) {
        DBCollection coll=getJobStore();
        DBObject q=new BasicDBObject("jobId",jobId).append("type","job");
        DBObject doc=coll.findOne(q,new BasicDBObject(),ReadPreference.primary());
        if(doc!=null) {
            AsynchResponse response=fromBson(doc);
            if(response.getAsynchStatus()==AsynchStatus.completed||
               response.getAsynchStatus()==AsynchStatus.timedout) {
                coll.remove(q);
            }
            return response;
        } else {
            return null;
        }
    }

    @Override
    public AsynchronousJob getAndLockNextAsynchronousJob() {
        DBCollection coll=getJobStore();
        Date now=new Date();
        DBObject q=new BasicDBObject("asynchStatus",AsynchStatus.scheduled.toString()).
            append("scheduledTime",new BasicDBObject("$lte",now));
        DBObject s=new BasicDBObject("priority","-1");
        DBObject u=new BasicDBObject("$set",
                                     new BasicDBObject("asynchStatus",AsynchStatus.executing.toString()).
                                     append("executionStartTime",now));
        DBObject doc=coll.findAndModify(q,s,false,u,true,false);
        if(doc!=null) {
            return fromBson(doc);
        }
        return null;
    }

    @Override
    public void updateAsynchronousJob(AsynchronousJob job) {
        
    }

    public DBCollection getJobStore() {
    }

    private DBObject jobToBson(AsynchRequest request) {
        BasicDBObject doc=new BasicDBObject();
        
    }
}
