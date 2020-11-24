package com.qcloud.cmq;

import java.util.TreeMap;
import java.util.Vector;
import java.util.List;
import java.lang.Integer;

import com.qcloud.cmq.json.JSONArray;
import com.qcloud.cmq.json.JSONObject;

/**
 * TODO topic class.
 *
 * @author York.
 * Created 2016年9月26日.
 */
public class Topic {
    // topic name
    protected String topicName;
    // cmq client
    protected CMQClientInterceptor.Chain client;

    /**
     * construct .
     *
     * @param topicName String
     * @param client    CMQClient
     */
    Topic(String topicName, CMQClientInterceptor.Chain client) {
        this.topicName = topicName;
        this.client = client;
    }

    /**
     * TODO set topic attributes
     *
     * @param maxMsgSize int
     * @throws Exception
     */
    public void setTopicAttributes(final int maxMsgSize) throws Exception {
        if (maxMsgSize < 1024 || maxMsgSize > 1048576) {
            throw new CMQClientException("Invalid parameter maxMsgSize < 1KB or maxMsgSize > 1024KB");
        }

        TreeMap<String, String> param = new TreeMap<String, String>();

        param.put("topicName", this.topicName);

        if (maxMsgSize > 0) {
            param.put("maxMsgSize", Integer.toString(maxMsgSize));
        }

        String result = this.client.call("SetTopicAttributes", param);
        CMQTool.checkResult(result);
    }


    /**
     * TODO get topic attributes.
     *
     * @return return topic meta object
     * @throws Exception
     */
    public TopicMeta getTopicAttributes() throws Exception {
        TreeMap<String, String> param = new TreeMap<String, String>();

        param.put("topicName", this.topicName);
        String result = this.client.call("GetTopicAttributes", param);
        JSONObject jsonObj = new JSONObject(result);
        CMQTool.checkResult(result);

        TopicMeta meta = new TopicMeta();
        meta.msgCount = jsonObj.getInt("msgCount");
        meta.maxMsgSize = jsonObj.getInt("maxMsgSize");
        meta.msgRetentionSeconds = jsonObj.getInt("msgRetentionSeconds");
        meta.createTime = jsonObj.getInt("createTime");
        meta.lastModifyTime = jsonObj.getInt("lastModifyTime");
        meta.filterType = jsonObj.getInt("filterType");
        return meta;

    }

    /**
     * publish message without  tags.
     *
     * @param message String
     * @return msgId, String
     * @throws Exception
     */
    public String publishMessage(String message) throws Exception {
        return publishMessage(message, null, null);
    }

    public String publishMessage(String message, String routingKey) throws Exception {
        return publishMessage(message, null, routingKey);
    }

    /**
     * publish message .
     *
     * @param msg      String message body
     * @param vTagList Vector<String>  message tag
     * @return msgId String
     * @throws Exception
     */
    public String publishMessage(String msg, List<String> vTagList, String routingKey) throws Exception {
        TreeMap<String, String> param = new TreeMap<String, String>();

        param.put("topicName", this.topicName);
        param.put("msgBody", msg);
        if (routingKey != null) {
            param.put("routingKey", routingKey);
        }

        if (vTagList != null) {
            for (int i = 0; i < vTagList.size(); ++i) {
                param.put("msgTag." + Integer.toString(i + 1), vTagList.get(i));
            }
        }

        String result = this.client.call("PublishMessage", param);
        JSONObject jsonObj = new JSONObject(result);
        CMQTool.checkResult(result);
        return jsonObj.getString("msgId");
    }

    /**
     * TODO batch publish message without tags.
     *
     * @param vMsgList Vector<String> message array
     * @return msgId Vector<String> message id array
     * @throws Exception
     */
    public Vector<String> batchPublishMessage(List<String> vMsgList) throws Exception {
        return batchPublishMessage(vMsgList, null, null);
    }

    public Vector<String> batchPublishMessage(List<String> vMsgList, String routingKey) throws Exception {
        return batchPublishMessage(vMsgList, null, routingKey);
    }

    /**
     * batch publish message
     *
     * @param vMsgList               message array
     * @param vTagList               message tag array
     * @return message handles array
     * @throws Exception
     */
    public Vector<String> batchPublishMessage(List<String> vMsgList, List<String> vTagList, String routingKey) throws Exception {

        TreeMap<String, String> param = new TreeMap<String, String>();

        param.put("topicName", this.topicName);
        if (routingKey != null) {
            param.put("routingKey", routingKey);
        }

        if (vMsgList != null) {
            for (int i = 0; i < vMsgList.size(); ++i) {
                param.put("msgBody." + Integer.toString(i + 1), vMsgList.get(i));
            }
        }
        if (vTagList != null) {
            for (int i = 0; i < vTagList.size(); ++i) {
                param.put("msgTag." + Integer.toString(i + 1), vTagList.get(i));
            }
        }

        String result = this.client.call("BatchPublishMessage", param);
        JSONObject jsonObj = new JSONObject(result);
        CMQTool.checkResult(result);

        JSONArray jsonArray = jsonObj.getJSONArray("msgList");

        Vector<String> vmsgId = new Vector<String>();
        for (int i = 0; i < jsonArray.length(); i++) {
            JSONObject obj = (JSONObject) jsonArray.get(i);
            vmsgId.add(obj.getString("msgId"));
        }

        return vmsgId;

    }

    /**
     * TODO list subscription by topic.
     *
     * @param offset            int
     * @param limit             int
     * @param searchWord        String
     * @param vSubscriptionList List<String>
     * @return totalCount          int
     * @throws Exception
     */
	public int ListSubscription(final int offset, int limit,
								final String searchWord, List<String> vSubscriptionList
	) throws Exception {
        TreeMap<String, String> param = new TreeMap<String, String>();
        param.put("topicName", this.topicName);
        if (searchWord != null && !"".equals(searchWord)) {
            param.put("searchWord", searchWord);
        }
        if (offset >= 0) {
            param.put("offset", Integer.toString(offset));
        }
        if (limit > 0) {
            param.put("limit", Integer.toString(limit));
        }

        String result = this.client.call("ListSubscriptionByTopic", param);
        JSONObject jsonObj = new JSONObject(result);
        CMQTool.checkResult(result);

        int totalCount = jsonObj.getInt("totalCount");
        if(!jsonObj.has("subscriptionList")){
            return 0;
        }
        JSONArray jsonArray = jsonObj.getJSONArray("subscriptionList");
        for (int i = 0; i < jsonArray.length(); i++) {
            JSONObject obj = (JSONObject) jsonArray.get(i);
            vSubscriptionList.add(obj.getString("subscriptionName"));
        }
        return totalCount;
    }

}
