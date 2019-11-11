package com.qcloud.cmq.entity;

/**
 * @author: feynmanlin
 * @date: 2019/11/6 1:05 下午
 */
public class CmqResponse {
    private String msgId;
    private String requestId;
    private int code;

    public String getMsgId() {
        return msgId;
    }

    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }
}
