package com.qcloud.cmq;

import java.util.*;
import java.lang.Integer;

import com.qcloud.cmq.entity.CmqConfig;
import com.qcloud.cmq.json.*;

/**
 * CMQ_jAVA_SDK_V1.0.2
 * Update: Add topic and subscription function
 * Tips:
 * 1 Account class object is not safty in multy thread.
 * 2 You should init different account for topic or queue
 * Account class
 *
 * @author York.
 * Created 2016年9月27日.
 */
public class Account {


    protected CMQClientInterceptor.Chain client;

    public Account(String endpoint, String secretId, String secretKey) {
        this(new CmqConfig(endpoint, secretId, secretKey));
    }

    public Account(String secretId, String secretKey, String endpoint, String path, String method) {
        this(new CmqConfig(endpoint, secretId, secretKey, path, method));
    }

    public Account(CmqConfig cmqConfig) {
        this(cmqConfig, Arrays.asList());
    }

    public Account(CmqConfig cmqConfig, List<CMQClientInterceptor> interceptors) {
        this.client = new CMQClientInterceptor.Chains(new CMQClient(cmqConfig), interceptors);
    }

    public void createQueue(String queueName, QueueMeta meta) throws Exception {
        TreeMap<String, String> param = new TreeMap<String, String>();
        if ("".equals(queueName)) {
            throw new CMQClientException("Invalid parameter:queueName is empty");
        } else {
            param.put("queueName", queueName);
        }

        if (meta.maxMsgHeapNum > 0) {
            param.put("maxMsgHeapNum", Integer.toString(meta.maxMsgHeapNum));
        }
        if (meta.pollingWaitSeconds > 0) {
            param.put("pollingWaitSeconds", Integer.toString(meta.pollingWaitSeconds));
        }
        if (meta.visibilityTimeout > 0) {
            param.put("visibilityTimeout", Integer.toString(meta.visibilityTimeout));
        }
        if (meta.maxMsgSize > 0) {
            param.put("maxMsgSize", Integer.toString(meta.maxMsgSize));
        }
        if (meta.msgRetentionSeconds > 0) {
            param.put("msgRetentionSeconds", Integer.toString(meta.msgRetentionSeconds));
        }
        if (meta.rewindSeconds > 0) {
            param.put("rewindSeconds", Integer.toString(meta.rewindSeconds));
        }

        String result = this.client.call("CreateQueue", param);
        CMQTool.checkResult(result);
    }

    /**
     * delete queue
     *
     * @param queueName String queue name
     * @throws CMQClientException
     * @throws CMQServerException
     */
    public void deleteQueue(String queueName) throws Exception {
        TreeMap<String, String> param = new TreeMap<String, String>();
        if ("".equals(queueName)) {
            throw new CMQClientException("Invalid parameter:queueName is empty");
        } else {
            param.put("queueName", queueName);
        }

        String result = this.client.call("DeleteQueue", param);
        CMQTool.checkResult(result);
    }

    /**
     * list queue
     *
     * @param searchWord String
     * @param offset     int
     * @param limit      int
     * @param queueList  List<String>
     * @return totalCount int
     * @throws Exception
     * @throws CMQClientException
     * @throws CMQServerException
     */
    public int listQueue(String searchWord, int offset, int limit, List<String> queueList) throws Exception {
        TreeMap<String, String> param = new TreeMap<String, String>();
        if (!"".equals(searchWord)) {
            param.put("searchWord", searchWord);
        }
        if (offset >= 0) {
            param.put("offset", Integer.toString(offset));
        }
        if (limit > 0) {
            param.put("limit", Integer.toString(limit));
        }

        String result = this.client.call("ListQueue", param);
        CMQTool.checkResult(result);
        JSONObject jsonObj = new JSONObject(result);
        int totalCount = jsonObj.getInt("totalCount");
        JSONArray jsonArray = jsonObj.getJSONArray("queueList");
        for (int i = 0; i < jsonArray.length(); i++) {
            JSONObject obj = (JSONObject) jsonArray.get(i);
            queueList.add(obj.getString("queueName"));
        }

        return totalCount;
    }

    /**
     * get Queue
     *
     * @param queueName String
     * @return Queue object
     */
    public Queue getQueue(String queueName) {
        return new Queue(queueName, this.client);
    }

    /**
     * get topic object
     *
     * @param topicName String
     * @return Topic object
     */
    public Topic getTopic(String topicName) {
        return new Topic(topicName, this.client);
    }


    /**
     * TODO create topic
     *
     * @param topicName  String
     * @param maxMsgSize int
     * @throws Exception
     */
    public void createTopic(final String topicName, final int maxMsgSize) throws Exception {
        createTopic(topicName, maxMsgSize, 1);
    }

    public void createTopic(final String topicName, final int maxMsgSize, int filterType) throws Exception {
        TreeMap<String, String> param = new TreeMap<String, String>();
        if ("".equals(topicName)) {
            throw new CMQClientException("Invalid parameter:topicName is empty");
        } else {
            param.put("topicName", topicName);
        }

        param.put("filterType", Integer.toString(filterType));
        if (maxMsgSize < 1024 || maxMsgSize > 1048576) {
            throw new CMQClientException("Invalid parameter: maxMsgSize > 1024KB or maxMsgSize < 1KB");
        }

        param.put("maxMsgSize", Integer.toString(maxMsgSize));
        String result = this.client.call("CreateTopic", param);
        CMQTool.checkResult(result);
    }


    /**
     * TODO delete topic
     *
     * @param topicName String
     * @throws Exception int
     */
    public void deleteTopic(final String topicName) throws Exception {
        TreeMap<String, String> param = new TreeMap<String, String>();
        if ("".equals(topicName)) {
            throw new CMQClientException("Invalid parameter:topicName is empty");
        } else {
            param.put("topicName", topicName);
        }

        String result = this.client.call("DeleteTopic", param);
        CMQTool.checkResult(result);
    }


    /**
     * TODO list topic
     *
     * @param searchWord String
     * @param vTopicList List<String>
     * @param offset     int
     * @param limit      int
     * @return totalCount int
     * @throws Exception
     */
    public int listTopic(final String searchWord, List<String> vTopicList, final int offset, final int limit) throws Exception {
        TreeMap<String, String> param = new TreeMap<String, String>();
        if (!"".equals(searchWord)) {
            param.put("searchWord", searchWord);
        }
        if (offset >= 0) {
            param.put("offset", Integer.toString(offset));
        }
        if (limit > 0) {
            param.put("limit", Integer.toString(limit));
        }

        String result = this.client.call("ListTopic", param);

        CMQTool.checkResult(result);
        JSONObject jsonObj = new JSONObject(result);

        int totalCount = jsonObj.getInt("totalCount");
        JSONArray jsonArray = jsonObj.getJSONArray("topicList");

        vTopicList.clear();
        for (int i = 0; i < jsonArray.length(); i++) {
            JSONObject obj = (JSONObject) jsonArray.get(i);
            vTopicList.add(obj.getString("topicName"));
        }
        return totalCount;
    }

    /**
     * TODO create subscribe
     *
     * @param topicName        String
     * @param subscriptionName String
     * @param Endpoint         String
     * @param Protocal         String
     * @throws Exception
     */
    public void createSubscribe(final String topicName, final String subscriptionName, final String Endpoint, final String Protocal) throws Exception {
        createSubscribe(topicName, subscriptionName, Endpoint, Protocal, null, null, "BACKOFF_RETRY", "JSON");

    }

    /**
     * TODO create subscribe
     *
     * @param topicName           String
     * @param subscriptionName    String
     * @param Endpoint            String
     * @param Protocal            String
     * @param FilterTag           List<String>
     * @param NotifyStrategy      String
     * @param NotifyContentFormat String
     * @throws Exception
     */
    public void createSubscribe(final String topicName, final String subscriptionName, final String Endpoint, final String Protocal,
                                final List<String> FilterTag, final List<String> bindingKey, final String NotifyStrategy, final String NotifyContentFormat) throws Exception {


        if (FilterTag != null && FilterTag.size() > 5) {
            throw new CMQClientException("Invalid parameter: Tag number > 5");
        }


        TreeMap<String, String> param = new TreeMap<String, String>();
        if ("".equals(topicName)) {
            throw new CMQClientException("Invalid parameter:topicName is empty");
        }

        param.put("topicName", topicName);

        if ("".equals(subscriptionName)) {
            throw new CMQClientException("Invalid parameter:subscriptionName is empty");
        }

        param.put("subscriptionName", subscriptionName);

        if ("".equals(Endpoint)) {
            throw new CMQClientException("Invalid parameter:Endpoint is empty");
        }

        param.put("endpoint", Endpoint);

        if ("".equals(Protocal)) {
            throw new CMQClientException("Invalid parameter:Protocal is empty");
        }

        param.put("protocol", Protocal);

        if ("".equals(NotifyStrategy)) {
            throw new CMQClientException("Invalid parameter:NotifyStrategy is empty");
        }

        param.put("notifyStrategy", NotifyStrategy);

        if ("".equals(NotifyContentFormat)) {
            throw new CMQClientException("Invalid parameter:NotifyContentFormat is empty");
        }
        param.put("notifyContentFormat", NotifyContentFormat);

        if (FilterTag != null) {
            for (int i = 0; i < FilterTag.size(); ++i) {
                param.put("filterTag." + Integer.toString(i + 1), FilterTag.get(i));
            }
        }
        if (bindingKey != null) {
            for (int i = 0; i < bindingKey.size(); ++i) {
                param.put("bindingKey." + Integer.toString(i + 1), bindingKey.get(i));
            }
        }

        String result = this.client.call("Subscribe", param);
        CMQTool.checkResult(result);
    }


    /**
     * delete subscription .
     *
     * @param topicName        String
     * @param subscriptionName String
     * @throws Exception
     */
    public void deleteSubscribe(final String topicName, final String subscriptionName) throws Exception {
        TreeMap<String, String> param = new TreeMap<String, String>();
        if ("".equals(topicName)) {
            throw new CMQClientException("Invalid parameter:topicName is empty");
        } else {
            param.put("topicName", topicName);
        }

        if ("".equals(subscriptionName)) {
            throw new CMQClientException("Invalid parameter:subscriptionName is empty");
        } else {
            param.put("subscriptionName", subscriptionName);
        }

        String result = this.client.call("Unsubscribe", param);
        CMQTool.checkResult(result);
    }


    /**
     * TODO get a subscription object.
     *
     * @param topicName        String
     * @param subscriptionName String
     * @return
     */
    public Subscription getSubscription(final String topicName, final String subscriptionName) {
        return new Subscription(topicName, subscriptionName, this.client);
    }
}
