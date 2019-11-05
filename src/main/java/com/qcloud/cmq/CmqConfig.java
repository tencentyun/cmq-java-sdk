package com.qcloud.cmq;

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

    //Whether to print a slow query
    private boolean printSlow = false;

    private boolean alwaysPrintResultLog = false;
    //Slow query threshold, unit ms
    private long slowThreshold = 1500;
    //maximum wait time for polling
    private int pollingTimeout;

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

    public int getPollingTimeout() {
        return pollingTimeout;
    }

    public void setPollingTimeout(int pollingTimeout) {
        this.pollingTimeout = pollingTimeout;
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
