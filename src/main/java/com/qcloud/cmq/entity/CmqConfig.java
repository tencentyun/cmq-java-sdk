package com.qcloud.cmq.entity;

import com.qcloud.cmq.CMQHttp;

/**
 * @author: feynmanlin
 * @date: 2019/11/5 2:30 下午
 */
public class CmqConfig {
    private String endpoint;
    private String path = "/v2/index.php";
    private String secretId;
    private String secretKey;
    private String method = "POST";
    private String signMethod = "sha1";
    private CMQHttp cmqHttp;

    //是否打印慢操作
    private boolean printSlow = true;
    //是否总是打印返回结果
    private boolean alwaysPrintResultLog = false;
    //慢操作阈值 ms
    private long slowThreshold = 1500;
    //最大连接等待时间 ms
    private int connectTimeout = 80000;
    //客户端读取数据，超时时间 ms，请务必大于控制台设置的队列长轮询时间
    private int readTimeout = 80000;
    //线程池中最大空闲线程数
    private int maxIdleConnections = 10;

    //预留配置项
    private boolean isReceive = false;
    //预留配置项，接收消息时，服务端长轮询挂起时间 ms
    private int pollingWaitTimeout = 30000;

    public boolean isReceive() {
        return isReceive;
    }

    public void setReceive(boolean receive) {
        isReceive = receive;
    }

    public int getPollingWaitTimeout() {
        return pollingWaitTimeout;
    }

    public void setPollingWaitTimeout(int pollingWaitTimeout) {
        this.pollingWaitTimeout = pollingWaitTimeout;
    }

    public int getReadTimeout() {
        return readTimeout;
    }

    public void setReadTimeout(int readTimeout) {
        this.readTimeout = readTimeout;
    }

    public int getMaxIdleConnections() {
        return maxIdleConnections;
    }

    public void setMaxIdleConnections(int maxIdleConnections) {
        this.maxIdleConnections = maxIdleConnections;
    }

    public boolean isAlwaysPrintResultLog() {
        return alwaysPrintResultLog;
    }

    public void setAlwaysPrintResultLog(boolean alwaysPrintResultLog) {
        this.alwaysPrintResultLog = alwaysPrintResultLog;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getSecretId() {
        return secretId;
    }

    public void setSecretId(String secretId) {
        this.secretId = secretId;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public String getSignMethod() {
        return signMethod;
    }

    public void setSignMethod(String signMethod) {
        this.signMethod = signMethod;
    }

    public CMQHttp getCmqHttp() {
        return cmqHttp;
    }

    public void setCmqHttp(CMQHttp cmqHttp) {
        this.cmqHttp = cmqHttp;
    }

    public int getConnectTimeout() {
        return connectTimeout;
    }

    public void setConnectTimeout(int connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    public boolean isPrintSlow() {
        return printSlow;
    }

    public void setPrintSlow(boolean printSlow) {
        this.printSlow = printSlow;
    }

    public long getSlowThreshold() {
        return slowThreshold;
    }

    public void setSlowThreshold(long slowThreshold) {
        this.slowThreshold = slowThreshold;
    }
}
