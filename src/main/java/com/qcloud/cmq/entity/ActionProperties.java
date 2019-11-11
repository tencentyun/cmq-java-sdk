package com.qcloud.cmq.entity;

/**
 * @author: feynmanlin
 * @date: 2019/11/8 12:12 下午
 */
public class ActionProperties {
    public static String POLLING = "POLLING";
    public static String POLLING_OLD = "POLLING_OLD";

    //操作类型，如长训轮操作、普通请求
    private String actionType;
    //长轮询等待时间
    private int pollingWaitSeconds;

    public String getActionType() {
        return actionType;
    }

    public void setActionType(String actionType) {
        this.actionType = actionType;
    }

    public int getPollingWaitSeconds() {
        return pollingWaitSeconds;
    }

    public void setPollingWaitSeconds(int pollingWaitSeconds) {
        this.pollingWaitSeconds = pollingWaitSeconds;
    }
}
