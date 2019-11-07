package com.qcloud.cmq;

import com.qcloud.cmq.entity.CmqConfig;

import java.net.URLEncoder;
import java.util.Random;
import java.util.TreeMap;

public class CMQClient {

    protected String CURRENT_VERSION = "SDK_JAVA_1.3";

    protected CmqConfig cmqConfig;

    public CMQClient(CmqConfig cmqConfig) {
        this.cmqConfig = cmqConfig;
    }

    public void setSignMethod(String signMethod) {
        if ("sha1".equals(signMethod) || "sha256".equals(signMethod)) {
            cmqConfig.setSignMethod(signMethod);
        } else {
            throw new CMQClientException("Only support sha256 or sha1");
        }
    }

    public String call(String action, TreeMap<String, String> param) throws Exception {
        if(cmqConfig == null){
            throw new RuntimeException("cmqConfig is null!");
        }
        return call(action, param, cmqConfig);
    }

    public String callReceive(String action, TreeMap<String, String> param) throws Exception {
        //todo 为了兼容老版本pollingWaitSeconds参数，对Receive对特殊处理，在后续版本删除receiveMessage(int pollingWaitSeconds)等接口后，可以删除
        CmqConfig tempCmqConfig = new CmqConfig();
        tempCmqConfig.setReceive(true);
        tempCmqConfig.setEndpoint(cmqConfig.getEndpoint());
        tempCmqConfig.setPath(cmqConfig.getPath());
        tempCmqConfig.setSecretId(cmqConfig.getSecretId());
        tempCmqConfig.setSecretKey(cmqConfig.getSecretKey());
        tempCmqConfig.setMethod(cmqConfig.getMethod());
        tempCmqConfig.setSignMethod(cmqConfig.getSignMethod());
        tempCmqConfig.setCmqHttp(cmqConfig.getCmqHttp());
        tempCmqConfig.setPrintSlow(cmqConfig.isPrintSlow());
        tempCmqConfig.setSlowThreshold(cmqConfig.getSlowThreshold());
        tempCmqConfig.setConnectTimeout(cmqConfig.getConnectTimeout());
        tempCmqConfig.setReadTimeout(cmqConfig.getReadTimeout());
        tempCmqConfig.setMaxIdleConnections(cmqConfig.getMaxIdleConnections());
        tempCmqConfig.setPollingWaitTimeout(Integer.parseInt(param.get("pollingWaitSeconds")));
        return call(action, param, tempCmqConfig);
    }

    public String call(String action, TreeMap<String, String> param, CmqConfig cmqConfig) throws Exception {
        String rsp = "";
        param.put("Action", action);
        param.put("Nonce", Integer.toString(new Random().nextInt(java.lang.Integer.MAX_VALUE)));
        param.put("SecretId", cmqConfig.getSecretId());
        param.put("Timestamp", Long.toString(System.currentTimeMillis() / 1000));
        param.put("RequestClient", this.CURRENT_VERSION);
        if ("sha256".equals(cmqConfig.getSignMethod())) {
            param.put("SignatureMethod", "HmacSHA256");
        } else {
            param.put("SignatureMethod", "HmacSHA1");
        }

        String host = "";
        if (cmqConfig.getEndpoint().startsWith("https")) {
            host = cmqConfig.getEndpoint().substring(8);
        } else {
            host = cmqConfig.getEndpoint().substring(7);
        }
        String src = "";
        src += cmqConfig.getMethod() + host + cmqConfig.getPath() + "?";

        boolean flag = false;
        for (String key : param.keySet()) {
            if (flag) {
                src += "&";
            }
            src += key.replace("_", ".") + "=" + param.get(key);
            flag = true;
        }
        param.put("Signature", CMQTool.sign(src, cmqConfig.getSecretKey(), cmqConfig.getSignMethod()));
        String url = "";
        String req = "";
        if ("GET".equals(cmqConfig.getMethod())) {
            url = cmqConfig.getEndpoint() + cmqConfig.getPath() + "?";
            flag = false;
            for (String key : param.keySet()) {
                if (flag) {
                    url += "&";
                }
                url += key + "=" + URLEncoder.encode(param.get(key), "utf-8");
                flag = true;
            }
            if (url.length() > 2048) {
                throw new CMQClientException("URL length is larger than 2K when use GET method");
            }
        } else {
            url = cmqConfig.getEndpoint() + cmqConfig.getPath();
            flag = false;
            for (String key : param.keySet()) {
                if (flag) {
                    req += "&";
                }
                req += key + "=" + URLEncoder.encode(param.get(key), "utf-8");
                flag = true;
            }
        }


        rsp = HttpUtil.request(url, req, cmqConfig);
        return rsp;
    }
}
