package com.qcloud.cmq;

import okhttp3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class HttpUtil {
    public static int timeout = 1000;
    public static String request(String method, String url, String data, CmqConfig cmqConfig) throws Exception{
        String result = "";
        if ("POST".equals(method)) {
            result = httpPost(url,data,cmqConfig);
        }
        else{
            result = httpGet(url, cmqConfig);
        }
        return result;
    }

    private static final Logger log = LoggerFactory.getLogger(HttpUtil.class);


    private static String httpGet(String url, CmqConfig cmqConfig) throws Exception{
        String result = null;
        OkHttpClient client = new OkHttpClient().newBuilder().connectTimeout(cmqConfig.getPollingTimeout()
                + timeout, TimeUnit.MILLISECONDS).readTimeout(cmqConfig.getPollingTimeout() + timeout, TimeUnit.MILLISECONDS).build();
        Request request = new Request.Builder().url(url).build();
        long start = System.currentTimeMillis();
        log.debug("request:{} timeout:{}",request.toString(),client.readTimeoutMillis());
        try {
            Response response = client.newCall(request).execute();
            long duration = System.currentTimeMillis() - start;
            result = response.body().string();
            if (log.isDebugEnabled()) {
                log.debug("exec time: {},response:{},request:{}", duration, result, url);
            } else if (cmqConfig.isAlwaysPrintResultLog()) {
                log.info("exec time: {},response:{}", duration, result);
            } else if (duration > cmqConfig.getSlowThreshold()) {
                log.warn("exec time: {},response:{},request:{}", duration, result, url);
            }
        } catch (Exception e) {
            long duration = System.currentTimeMillis() - start;
            log.error("exec time: {},request:{},err:{}", duration, url, e.getMessage());
            throw e;
        }
        return result;
    }

    /**
     * 发送httppost请求
     *
     * @param url
     * @param data  提交的参数为key=value&key1=value1的形式
     * @return
     */
    private static String httpPost(String url, String data, CmqConfig cmqConfig) throws Exception {
        String result;
        OkHttpClient httpClient = new OkHttpClient().newBuilder().connectTimeout(cmqConfig.getPollingTimeout()
                + timeout, TimeUnit.MILLISECONDS).readTimeout(cmqConfig.getPollingTimeout() + timeout, TimeUnit.MILLISECONDS).build();
        RequestBody requestBody = RequestBody.create(MediaType.parse("*/*;charset=utf-8"), data);
        Request request = new Request.Builder().url(url).post(requestBody).build();
        long start = System.currentTimeMillis();
        log.debug("request:{} timeout:{} data:{}",request.toString(), httpClient.readTimeoutMillis(),data);

        try {
            Response response = httpClient.newCall(request).execute();
            long duration = System.currentTimeMillis() - start;
            result = response.body().string();
            if (log.isDebugEnabled()) {
                log.debug("exec time: {},response:{},request:{}", duration, result, url);
            } else if (cmqConfig.isAlwaysPrintResultLog()) {
                log.info("exec time: {},response:{}", duration, result);
            } else if (duration > cmqConfig.getSlowThreshold()) {
                log.warn("exec time: {},response:{},request:{}", duration, result, url);
            }
        } catch (Exception e) {
            long duration = System.currentTimeMillis() - start;
            log.error("exec time: {},request:{},err:{}", duration, url, e.getMessage());
            throw e;
        }
        return result;
    }
}
