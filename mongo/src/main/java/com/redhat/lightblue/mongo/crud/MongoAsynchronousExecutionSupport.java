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

import java.util.Date;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.DBCollection;
import com.mongodb.ReadPreference;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import com.redhat.lightblue.Request;
import com.redhat.lightblue.Response;
import com.redhat.lightblue.AsynchRequest;
import com.redhat.lightblue.AsynchResponse;
import com.redhat.lightblue.AsynchStatus;
import com.redhat.lightblue.crud.FindRequest;
import com.redhat.lightblue.crud.UpdateRequest;
import com.redhat.lightblue.crud.SaveRequest;
import com.redhat.lightblue.crud.DeleteRequest;
import com.redhat.lightblue.crud.InsertionRequest;
import com.redhat.lightblue.crud.BulkRequest;
import com.redhat.lightblue.crud.CRUDOperation;

import com.redhat.lightblue.config.LightblueFactory;
import com.redhat.lightblue.config.LightblueFactoryAware;

import com.redhat.lightblue.extensions.asynch.AsynchronousExecutionSupport;
import com.redhat.lightblue.extensions.asynch.AsynchronousExecutionConfiguration;
import com.redhat.lightblue.extensions.asynch.AsynchronousJob;

import com.redhat.lightblue.mongo.common.MongoDataStore;

import com.redhat.lightblue.util.JsonUtils;
import com.redhat.lightblue.util.Error;

/**
 * This implementation of asynchronous execution support manages a job
 * store with three types of documents.
 * 
 *   job record: This is a small record with the job status information. 
 *           _id : jobId
 *           status
 *           priority
 *           dates
 *   job request data: This contains the request data submitted with the job. 
 *           requestOwner: points to jobId
 *           data
 *   job response data: This contains the response data, and there can be multiple.
 *           responseOwner: points to jobId
 *           seq: index of response chunk
 *           data
 */
public class MongoAsynchronousExecutionSupport implements AsynchronousExecutionSupport, LightblueFactoryAware {

    private LightblueFactory lbFactory;
    private AsynchronousExecutionConfiguration cfg;
    private DBCollection collection;

    public static final int SEGMENT_LENGTH=1000000;

    private static Set<String> initializedCollections = new CopyOnWriteArraySet<>();

    private static class JobRecord {
        String jobId;
        String status;
        int priority;
        Date createdTime;
        Date scheduledTime;
        Date executionStartTime;
        Date completionTime;
        Date timeoutTime;

        public DBObject toBson() {
            BasicDBObject doc=new BasicDBObject();
            append(doc,"_id",jobId);
            append(doc,"asyncStatus",status);
            append(doc,"priority",new Integer(priority));
            append(doc,"createdTime",createdTime);
            append(doc,"scheduledTime",scheduledTime);
            append(doc,"executionStartTime",executionStartTime);
            append(doc,"completionTime",completionTime);
            append(doc,"timeoutTime",timeoutTime);
            return doc;
        }

        public static JobRecord fromBson(DBObject doc) {
            JobRecord ret=new JobRecord();
            if(doc!=null) {
                ret.jobId=doc.get("_id").toString();
                ret.status=(String)doc.get("status");
                Number n=(Number)doc.get("priority");
                if(n!=null)
                    ret.priority=n.intValue();
                ret.createdTime=(Date)doc.get("createdTime");
                ret.scheduledTime=(Date)doc.get("scheduledTime");
                ret.executionStartTime=(Date)doc.get("executionStartTime");
                ret.completionTime=(Date)doc.get("completionTime");
                ret.timeoutTime=(Date)doc.get("timeoutTime");
            }
            return ret;
        }
    }

    private static class JobRequestData {
        String requestOwner;
        Request requestData;
        BulkRequest bulkRequestData;

        public DBObject toBson() {
            BasicDBObject doc=new BasicDBObject();
            append(doc,"requestOwner",requestOwner);
            if(requestData!=null) {
                append(doc,"requestData",requestData.toJson().toString());
                append(doc,"op",requestData.getOperation().toString());
            } else if(bulkRequestData!=null) {
                append(doc,"bulkRequestData",bulkRequestData.toJson().toString());
            }
            return doc;
        }

        public static JobRequestData fromBson(DBObject doc) {
            JobRequestData ret=new JobRequestData();
            if(doc!=null) {
                ret.requestOwner=doc.get("requestOwner").toString();
                String op=(String)doc.get("op");
                if(op!=null) {
                    String s=(String)doc.get("requestData");
                    if(s!=null) {
                        ObjectNode node=(ObjectNode)JsonUtils.json(s);
                        if (op.equalsIgnoreCase(CRUDOperation.FIND.toString())) {
                            ret.requestData = FindRequest.fromJson(node);
                        } else if (op.equalsIgnoreCase(CRUDOperation.INSERT.toString())) {
                            ret.requestData = InsertionRequest.fromJson(node);
                        } else if (op.equalsIgnoreCase(CRUDOperation.SAVE.toString())) {
                            ret.requestData = SaveRequest.fromJson(node);
                        } else if (op.equalsIgnoreCase(CRUDOperation.UPDATE.toString())) {
                            ret.requestData = UpdateRequest.fromJson(node);
                        } else if (op.equalsIgnoreCase(CRUDOperation.DELETE.toString())) {
                            ret.requestData = DeleteRequest.fromJson(node);
                        }
                    }
                } else {
                    String s=(String)doc.get("bulkRequestData");
                    if(s!=null)
                        ret.bulkRequestData=BulkRequest.fromJson((ObjectNode)JsonUtils.json(s));
                }
        }
            return ret;
        }
    }

    private static class JobResponseData {
        String responseOwner;
        int seq;
        boolean bulk;
        String data;

        public DBObject toBson() {
            BasicDBObject doc=new BasicDBObject();
            append(doc,"responseOwner",responseOwner);
            append(doc,"seq",seq);
            append(doc,"bulk",bulk);
            append(doc,"data",data);
            return doc;
        }

        public static JobResponseData fromBson(DBObject doc) {
            JobResponseData ret=new JobResponseData();
            if(doc!=null) {
                ret.responseOwner=doc.get("responseOwner").toString();
                Number n=(Number)doc.get("seq");
                if(n!=null)
                    ret.seq=n.intValue();
                Boolean b=(Boolean)doc.get("bulk");
                if(b!=null)
                    ret.bulk=b;
                ret.data=(String)doc.get("data");
            }
            return ret;
        }
    }
    
    private static BasicDBObject append(BasicDBObject obj,String name,Object value) {
        if(value!=null)
            obj.append(name,value);
        return obj;
    }
                
    public void init(DBCollection coll) {
        // Make sure we have our indexes
        if (!initializedCollections.contains(coll.getFullName())) {
            initializedCollections.add(coll.getFullName());
            BasicDBObject keys = new BasicDBObject("requestOwner", 1);
            BasicDBObject options = new BasicDBObject("sparse", 1);
            coll.createIndex(keys, options);

            keys=new BasicDBObject("responseOwner",1).append("seq",1);
            coll.createIndex(keys,options);

            keys=new BasicDBObject("asynchStatus",1).append("scheduledDate",-1);
            coll.createIndex(keys,options);
        }
    }

    @Override
    public void setLightblueFactory(LightblueFactory factory) {
        this.lbFactory=factory;
        this.cfg=lbFactory.getFactory().getAsynchronousExecutionConfiguration();
    }

    @Override
    public AsynchResponse scheduleAsynchronousExecution(AsynchRequest request) {
        DBCollection coll=getJobStore();
        Date now=new Date();

        JobRecord jobRecord=new JobRecord();
        jobRecord.createdTime=now;
        jobRecord.status=AsynchStatus.scheduled.toString();
        jobRecord.scheduledTime=request.getExecuteAfter()==null?now:request.getExecuteAfter();
        jobRecord.priority=request.getPriority();
        
        JobRequestData requestData=new JobRequestData();
        if(request.isBulk())
            requestData.bulkRequestData=request.getBulkRequest();
        else
            requestData.requestData=request.getRequest();
        
        AsynchResponse response=new AsynchResponse();
        response.setPriority(request.getPriority());
        response.setScheduledTime(now);
        response.setAsynchStatus(AsynchStatus.scheduled);
        try {
            DBObject jobDoc=jobRecord.toBson();
            coll.insert(jobDoc);
            response.setJobId(jobDoc.get("_id").toString());
            requestData.requestOwner=response.getJobId();
            coll.insert(requestData.toBson());
        } catch (Exception e) {
            throw Error.get(MongoCrudConstants.ERR_ASYNCH_SCHEDULING,e.toString());
        }

        return response;
    }

    @Override
    public AsynchResponse getAsynchronousExecutionStatus(String jobId) {
        DBCollection coll=getJobStore();

        JobRecord jobDoc=new JobRecord();
        jobDoc.jobId=jobId;
        DBObject q=jobDoc.toBson();
        DBObject doc=coll.findOne(q,new BasicDBObject(),ReadPreference.primary());
        if(doc!=null) {
            AsynchResponse response=new AsynchResponse();
            jobDoc=JobRecord.fromBson(doc);
            response.setJobId(jobDoc.jobId);
            response.setPriority(jobDoc.priority);
            response.setScheduledTime(jobDoc.scheduledTime);
            response.setExecutionStartTime(jobDoc.executionStartTime);
            response.setCompletionTime(jobDoc.completionTime);
            response.setTimeoutTime(jobDoc.timeoutTime);
            response.setAsynchStatus(AsynchStatus.valueOf(jobDoc.status));
            if(AsynchStatus.completed.toString().equals(jobDoc.status)) {
                coll.remove(q);
                coll.remove(new BasicDBObject("requestOwner",jobId));
                buildResponseData(coll,jobId,response);
                coll.remove(new BasicDBObject("responseOwner",jobId));
            } else if(AsynchStatus.timedout.toString().equals(jobDoc.status)) {
                coll.remove(q);
                coll.remove(new BasicDBObject("requestOwner",jobId));
                coll.remove(new BasicDBObject("responseOwner",jobId));
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
        DBObject doc=coll.findAndModify(q,new BasicDBObject(),s,false,u,true,false);
        if(doc!=null) {
            AsynchronousJob ret=new AsynchronousJob();
            JobRecord jobDoc=JobRecord.fromBson(doc);
            ret.jobId=jobDoc.jobId;
            ret.priority=jobDoc.priority;
            ret.createdTime=jobDoc.createdTime;
            ret.scheduledTime=jobDoc.scheduledTime;
            ret.executionStartTime=jobDoc.executionStartTime;
            ret.asynchStatus=AsynchStatus.valueOf(jobDoc.status);

            DBObject reqDoc=coll.findOne(new BasicDBObject("requestOwner",jobDoc.jobId),new BasicDBObject(),ReadPreference.primary());
            JobRequestData reqData=JobRequestData.fromBson(reqDoc);
            if(reqData.requestData!=null) {
                ret.singleData=new AsynchronousJob.SingleRequestData();
                ret.singleData.request=reqData.requestData;
            } else {
                ret.bulkData=new AsynchronousJob.BulkRequestData();
                ret.bulkData.request=reqData.bulkRequestData;
            }
            
            return ret;
        }
        return null;
    }

    @Override
    public void completeJob(AsynchronousJob job) {
        DBCollection coll=getJobStore();

        String response=null;
        boolean bulk;
        if(job.singleData!=null&&job.singleData.response!=null) {
            response=job.singleData.response.toJson().toString();
            bulk=false;
        } else if(job.bulkData!=null&&job.bulkData.response!=null) {
            response=job.bulkData.response.toJson().toString();
            bulk=true;
        }
        if(response!=null) {
            int seq=0;
            int len=response.length();
            int off=0;
            while(len>0) {
                JobResponseData data=new JobResponseData();
                data.responseOwner=job.jobId;
                data.seq=seq++;
                data.bulk=bulk;
                int n=len>SEGMENT_LENGTH?SEGMENT_LENGTH:len;
                data.data=response.substring(off,off+n);
                coll.insert(data.toBson());
                off+=n;
                len-=n;
            }
        }
        coll.findAndModify(new BasicDBObject("_id",job.jobId),
                           new BasicDBObject("$set",new BasicDBObject("asynchStatus",AsynchStatus.completed.toString()).
                                             append("completionTime",new Date())));
                           
    }

    public DBCollection getJobStore() {
        if(collection!=null)
            return collection;
        
        String msg=null;
        if(cfg.getOptions()!=null) {
            MongoDataStore store=new MongoDataStore();
            JsonNode node=cfg.getOptions().get("datasource");
            int n=0;
            if(node!=null) {
                store.setDatasourceName(node.asText());
                n++;
            }
            node=cfg.getOptions().get("database");
            if(node!=null) {
                store.setDatabaseName(node.asText());
                n++;
            }
            if(n==0)
                msg="datasource or database required";
            else {
                DB db=resolver.get(store);
                node=cfg.getOptions().get("collection");
                if(node!=null) {
                    collection=db.getCollection(node.asText());
                    init(collection);
                    return collection;
                } else
                    msg="collection required";
            }
        } else {
            msg="No asynchronous execution options";
        }
        throw Error.get(MongoCrudConstants.ERR_ASYNCH_CONFIG,msg);
    }

}
