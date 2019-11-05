package com.qcloud.cmq;

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
