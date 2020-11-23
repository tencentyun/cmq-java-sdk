package com.qcloud.cmq;

import com.qcloud.cmq.entity.CmqResponse;
import com.qcloud.cmq.json.JSONArray;
import com.qcloud.cmq.json.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

/**
 * Queue class.
 *
 * @author York.
 * Created 2016年9月26日.
 */
public class Queue {
    protected String queueName;
    protected CMQClientInterceptor.Chain client;


    Queue(String queueName, CMQClientInterceptor.Chain client) {
        this.queueName = queueName;
        this.client = client;
    }

    /**
     * 设置队列属性
     *
     * @param meta 队列属性参数
     * @throws CMQClientException
     * @throws CMQServerException
     */
    public void setQueueAttributes(QueueMeta meta) throws Exception {
        TreeMap<String, String> param = new TreeMap<String, String>();

        param.put("queueName", this.queueName);

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
        String result = this.client.call("SetQueueAttributes", param);
        CMQTool.checkResult(result);
    }


    /**
     * 获取队列属性
     *
     * @return 返回的队列属性参数
     */
    public QueueMeta getQueueAttributes() throws Exception {
        TreeMap<String, String> param = new TreeMap<String, String>();

        param.put("queueName", this.queueName);
        String result = this.client.call("GetQueueAttributes", param);
        JSONObject jsonObj = new JSONObject(result);
        CMQTool.checkResult(result);

        QueueMeta meta = new QueueMeta();
        meta.maxMsgHeapNum = jsonObj.getInt("maxMsgHeapNum");
        meta.pollingWaitSeconds = jsonObj.getInt("pollingWaitSeconds");
        meta.visibilityTimeout = jsonObj.getInt("visibilityTimeout");
        meta.maxMsgSize = jsonObj.getInt("maxMsgSize");
        meta.msgRetentionSeconds = jsonObj.getInt("msgRetentionSeconds");
        meta.createTime = jsonObj.getInt("createTime");
        meta.lastModifyTime = jsonObj.getInt("lastModifyTime");
        meta.activeMsgNum = jsonObj.getInt("activeMsgNum");
        meta.inactiveMsgNum = jsonObj.getInt("inactiveMsgNum");
        meta.rewindmsgNum = jsonObj.getInt("rewindMsgNum");
        meta.minMsgTime = jsonObj.getInt("minMsgTime");
        meta.delayMsgNum = jsonObj.getInt("delayMsgNum");
        meta.rewindSeconds = jsonObj.getInt("rewindSeconds");


        return meta;
    }

    @Deprecated
    public String sendMessage(String msgBody) throws Exception {
        return sendMessage(msgBody, 0);
    }

    /**
     * 发送消息,接口在1.0.7中将被废弃
     * @param msgBody 消息内容
     * @return 服务器返回的消息唯一标识
     */
    @Deprecated
    public String sendMessage(String msgBody, int delayTime) throws Exception {
        TreeMap<String, String> param = new TreeMap<String, String>();

        param.put("queueName", this.queueName);
        param.put("msgBody", msgBody);
        param.put("delaySeconds", Integer.toString(delayTime));

        String result = this.client.call("SendMessage", param);
        JSONObject jsonObj = new JSONObject(result);
        CMQTool.checkResult(result);

        return jsonObj.getString("msgId");
    }

    public CmqResponse send(String msgBody) throws Exception {
        return send(msgBody, 0);
    }

    public CmqResponse send(String msgBody, int delayTime) throws Exception {
        TreeMap<String, String> param = new TreeMap<>();

        param.put("queueName", this.queueName);
        param.put("msgBody", msgBody);
        param.put("delaySeconds", Integer.toString(delayTime));

        String result = this.client.call("SendMessage", param);
        JSONObject jsonObj = new JSONObject(result);
        CMQTool.checkResult(result);

        CmqResponse cmqResponse = new CmqResponse();
        cmqResponse.setCode(jsonObj.getInt("code"));
        cmqResponse.setMsgId(jsonObj.getString("msgId"));
        cmqResponse.setRequestId(jsonObj.getString("requestId"));

        return cmqResponse;
    }
    @Deprecated
    public List<String> batchSendMessage(List<String> vtMsgBody) throws Exception {
        return batchSendMessage(vtMsgBody, 0);
    }

    /**
     * 批量发送消息,接口在1.0.7中将被废弃
     * @param vtMsgBody 消息列表
     * @return 服务器返回的消息唯一标识列表
     */
    @Deprecated
    public List<String> batchSendMessage(List<String> vtMsgBody, int delayTime) throws Exception {

        if (vtMsgBody.isEmpty() || vtMsgBody.size() > 16) {
            throw new CMQClientException("Error: message size is empty or more than 16");
        }

        TreeMap<String, String> param = new TreeMap<String, String>();

        param.put("queueName", this.queueName);
        for (int i = 0; i < vtMsgBody.size(); i++) {
            String k = "msgBody." + Integer.toString(i + 1);
            param.put(k, vtMsgBody.get(i));
        }
        param.put("delaySeconds", Integer.toString(delayTime));
        String result = this.client.call("BatchSendMessage", param);
        JSONObject jsonObj = new JSONObject(result);
        CMQTool.checkResult(result);

        ArrayList<String> vtMsgId = new ArrayList<String>();
        JSONArray jsonArray = jsonObj.getJSONArray("msgList");
        for (int i = 0; i < jsonArray.length(); i++) {
            JSONObject obj = (JSONObject) jsonArray.get(i);
            vtMsgId.add(obj.getString("msgId"));
        }

        return vtMsgId;
    }

    public List<CmqResponse> batchSend(List<String> vtMsgBody) throws Exception {
        return batchSend(vtMsgBody, 0);
    }

    public List<CmqResponse> batchSend(List<String> vtMsgBody, int delayTime) throws Exception {
        if (vtMsgBody.isEmpty() || vtMsgBody.size() > 16) {
            throw new CMQClientException("Error: message size is empty or more than 16");
        }
        TreeMap<String, String> param = new TreeMap<String, String>();

        param.put("queueName", this.queueName);
        for (int i = 0; i < vtMsgBody.size(); i++) {
            String k = "msgBody." + (i + 1);
            param.put(k, vtMsgBody.get(i));
        }
        param.put("delaySeconds", Integer.toString(delayTime));
        String result = this.client.call("BatchSendMessage", param);
        JSONObject jsonObj = new JSONObject(result);
        CMQTool.checkResult(result);

        ArrayList<CmqResponse> cmqResponses = new ArrayList<>();
        JSONArray jsonArray = jsonObj.getJSONArray("msgList");
        String requestId = jsonObj.getString("requestId");
        int code = jsonObj.getInt("code");
        for (int i = 0; i < jsonArray.length(); i++) {
            JSONObject obj = (JSONObject) jsonArray.get(i);
            CmqResponse cmqResponse = new CmqResponse();
            cmqResponse.setRequestId(requestId);
            cmqResponse.setMsgId(obj.getString("msgId"));
            cmqResponse.setCode(code);
            cmqResponses.add(cmqResponse);
        }
        return cmqResponses;
    }

    /**
     * 接口在1.0.7中将被废弃，长轮询时间应该由queue端设置，请勿使用
     * @param pollingWaitSeconds
     * @return
     * @throws Exception
     */
    @Deprecated
    public Message receiveMessage(int pollingWaitSeconds) throws Exception {
        TreeMap<String, String> param = new TreeMap<>();

        param.put("queueName",this.queueName);
        if (pollingWaitSeconds >= 0) {
            param.put("pollingWaitSeconds", Integer.toString(pollingWaitSeconds));
        } else {
            param.put("pollingWaitSeconds", Integer.toString(30000));
        }

        String result = this.client.call("ReceiveMessage", param);
        JSONObject jsonObj = new JSONObject(result);
        int code = jsonObj.getInt("code");
        if(code != 0) {
            throw new CMQServerException(code,jsonObj.getString("message"));
        }

        Message msg = new Message();
        msg.msgId = jsonObj.getString("msgId");
        msg.receiptHandle = jsonObj.getString("receiptHandle");
        msg.msgBody = jsonObj.getString("msgBody");
        msg.enqueueTime = jsonObj.getLong("enqueueTime");
        msg.nextVisibleTime = jsonObj.getLong("nextVisibleTime");
        msg.firstDequeueTime = jsonObj.getLong("firstDequeueTime");
        msg.dequeueCount = jsonObj.getInt("dequeueCount");

        return msg;
    }

    /**
     * 批量获取消息，接口在1.0.7中将被废弃，长轮询时间应该由queue端设置，请勿使用
     *
     * @param numOfMsg               准备获取消息数
     * @param pollingWaitSeconds     请求最长的Polling等待时间
     * @return                       服务器返回消息列表
     * @throws CMQClientException
     * @throws CMQServerException
     */
    @Deprecated
    public List<Message> batchReceiveMessage(int numOfMsg, int pollingWaitSeconds) throws Exception {
        TreeMap<String, String> param = new TreeMap<>();

        param.put("queueName",this.queueName);
        param.put("numOfMsg",Integer.toString(numOfMsg));
        if (pollingWaitSeconds >= 0) {
            param.put("pollingWaitSeconds", Integer.toString(pollingWaitSeconds));
        } else {
            param.put("pollingWaitSeconds", Integer.toString(30000));
        }
        String result = this.client.call("BatchReceiveMessage", param);
        JSONObject jsonObj = new JSONObject(result);
        int code = jsonObj.getInt("code");
        if(code != 0) {
            throw new CMQServerException(code,jsonObj.getString("message"));
        }

        ArrayList<Message> vtMessage = new ArrayList<Message>();

        JSONArray jsonArray = jsonObj.getJSONArray("msgInfoList");
        for(int i=0;i<jsonArray.length();i++)
        {
            JSONObject obj = (JSONObject)jsonArray.get(i);
            Message msg = new Message();
            msg.msgId = obj.getString("msgId");
            msg.receiptHandle = obj.getString("receiptHandle");
            msg.msgBody = obj.getString("msgBody");
            msg.enqueueTime = obj.getLong("enqueueTime");
            msg.nextVisibleTime = obj.getLong("nextVisibleTime");
            msg.firstDequeueTime = obj.getLong("firstDequeueTime");
            msg.dequeueCount = obj.getInt("dequeueCount");

            vtMessage.add(msg);
        }

        return vtMessage;
    }

    /**
     * 获取消息
     * @return 服务器返回消息
     */
    public Message receiveMessage() throws Exception {
        TreeMap<String, String> param = new TreeMap<String, String>();

        param.put("queueName", this.queueName);

        String result = this.client.call("ReceiveMessage", param);
        JSONObject jsonObj = new JSONObject(result);
        CMQTool.checkResult(result);

        Message msg = new Message();
        msg.msgId = jsonObj.getString("msgId");
        msg.receiptHandle = jsonObj.getString("receiptHandle");
        msg.msgBody = jsonObj.getString("msgBody");
        msg.enqueueTime = jsonObj.getLong("enqueueTime");
        msg.nextVisibleTime = jsonObj.getLong("nextVisibleTime");
        msg.firstDequeueTime = jsonObj.getLong("firstDequeueTime");
        msg.dequeueCount = jsonObj.getInt("dequeueCount");
        msg.requestId = jsonObj.getString("requestId");

        return msg;
    }

    /**
     * 批量获取消息
     *
     * @param numOfMsg 准备获取消息数
     * @return 服务器返回消息列表
     * @throws CMQClientException
     * @throws CMQServerException
     */
    public List<Message> batchReceiveMessage(int numOfMsg) throws Exception {
        TreeMap<String, String> param = new TreeMap<String, String>();

        param.put("queueName", this.queueName);
        param.put("numOfMsg", Integer.toString(numOfMsg));

        String result = this.client.call("BatchReceiveMessage", param);
        JSONObject jsonObj = new JSONObject(result);
        CMQTool.checkResult(result);

        ArrayList<Message> vtMessage = new ArrayList<Message>();

        JSONArray jsonArray = jsonObj.getJSONArray("msgInfoList");
        for (int i = 0; i < jsonArray.length(); i++) {
            JSONObject obj = (JSONObject) jsonArray.get(i);
            Message msg = new Message();
            msg.msgId = obj.getString("msgId");
            msg.receiptHandle = obj.getString("receiptHandle");
            msg.msgBody = obj.getString("msgBody");
            msg.enqueueTime = obj.getLong("enqueueTime");
            msg.nextVisibleTime = obj.getLong("nextVisibleTime");
            msg.firstDequeueTime = obj.getLong("firstDequeueTime");
            msg.dequeueCount = obj.getInt("dequeueCount");
            msg.requestId = jsonObj.getString("requestId");

            vtMessage.add(msg);
        }

        return vtMessage;
    }

    /**
     * 删除消息
     *
     * @param receiptHandle 消息句柄,获取消息时由服务器返回
     * @throws CMQClientException
     * @throws CMQServerException
     */
    public CmqResponse deleteMessage(String receiptHandle) throws Exception {
        TreeMap<String, String> param = new TreeMap<String, String>();

        param.put("queueName", this.queueName);
        param.put("receiptHandle", receiptHandle);
        String result = this.client.call("DeleteMessage", param);
        CMQTool.checkResult(result);
        JSONObject jsonObject = new JSONObject(result);
        CmqResponse cmqResponse = new CmqResponse();
        cmqResponse.setRequestId(jsonObject.getString("requestId"));
        return cmqResponse;
    }

    /**
     * 批量删除消息
     *
     * @param vtReceiptHandle 消息句柄列表，获取消息时由服务器返回
     * @throws CMQClientException
     * @throws CMQServerException
     */
    public void batchDeleteMessage(List<String> vtReceiptHandle) throws Exception {
        if (vtReceiptHandle.isEmpty()) {
            return;
        }

        TreeMap<String, String> param = new TreeMap<String, String>();

        param.put("queueName", this.queueName);
        for (int i = 0; i < vtReceiptHandle.size(); i++) {
            String k = "receiptHandle." + (i + 1);
            param.put(k, vtReceiptHandle.get(i));
        }

        String result = this.client.call("BatchDeleteMessage", param);
        CMQTool.checkResult(result);
    }

    /**
     * 回溯队列
     *
     * @param backTrackingTime
     * @throws CMQClientException
     * @throws CMQServerException
     */

    public void rewindQueue(int backTrackingTime) throws Exception {
        if (backTrackingTime <= 0) {
            return;
        }

        TreeMap<String, String> param = new TreeMap<String, String>();

        param.put("queueName", this.queueName);
        param.put("startConsumeTime", Integer.toString(backTrackingTime));

        String result = this.client.call("RewindQueue", param);
        CMQTool.checkResult(result);
    }

}
