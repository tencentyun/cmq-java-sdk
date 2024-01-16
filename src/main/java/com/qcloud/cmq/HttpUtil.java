package com.qcloud.cmq;

import com.qcloud.cmq.entity.ActionProperties;
import com.qcloud.cmq.entity.CmqConfig;
import okhttp3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class HttpUtil {
    private static final Logger log = LoggerFactory.getLogger(HttpUtil.class);

    private static volatile OkHttpClient httpClient;
    private static volatile OkHttpClient receiveHttpClient;

    public static String request(String url, String data, CmqConfig cmqConfig, ActionProperties actionProperties) throws Exception {
        initHttpClient(cmqConfig);
        String result = "";
        if ("POST".equals(cmqConfig.getMethod())) {
            result = httpPost(url, data, cmqConfig, actionProperties);
        } else {
            result = httpGet(url, cmqConfig, actionProperties);
        }
        return result;
    }

    private static void initHttpClient(CmqConfig cmqConfig) {
        if (httpClient == null) {
            synchronized (HttpUtil.class) {
                if (httpClient == null) {
                    httpClient = new OkHttpClient().newBuilder()
                            .connectionPool(new ConnectionPool(cmqConfig.getMaxIdleConnections(), 5L, TimeUnit.MINUTES))
                            .connectTimeout(cmqConfig.getConnectTimeout(), TimeUnit.MILLISECONDS)
                            .readTimeout(cmqConfig.getReadTimeout(), TimeUnit.MILLISECONDS).build();
                }
            }
        }
        if (receiveHttpClient == null) {
            synchronized (HttpUtil.class) {
                if (receiveHttpClient == null) {
                    receiveHttpClient = new OkHttpClient().newBuilder()
                            .connectionPool(new ConnectionPool(cmqConfig.getMaxIdleConnections(), 5L, TimeUnit.MINUTES))
                            .connectTimeout(cmqConfig.getConnectTimeout(), TimeUnit.MILLISECONDS)
                            .readTimeout(cmqConfig.getReceiveTimeout(), TimeUnit.MILLISECONDS).build();
                }
            }
        }
    }

    private static String httpGet(String url, CmqConfig cmqConfig, ActionProperties actionProperties) throws Exception {
        String result = null;
        Request request = new Request.Builder().url(url).build();
        if (log.isDebugEnabled()) {
            log.debug("request:{} timeout:{}", request.toString(), httpClient.readTimeoutMillis());
        }
        result = doRequest(cmqConfig, request, actionProperties);
        return result;
    }

    private static String doRequest(CmqConfig cmqConfig, Request request,ActionProperties actionProperties) throws IOException {
        String result = null;
        Response response = null;
        long start = System.currentTimeMillis();
        try {
            // 为了兼容老接口 pollingWaitSeconds 支持自定义设置超过30s, 存在fd耗尽风险
            if (ActionProperties.POLLING_OLD.equals(actionProperties.getActionType())) {
                int pollingWaitTime = actionProperties.getPollingWaitSeconds() >= 0 ? actionProperties.getPollingWaitSeconds() * 1000 : 0;
                OkHttpClient okHttpClient = new OkHttpClient().newBuilder()
                        .connectionPool(new ConnectionPool(1, 5L, TimeUnit.MINUTES))
                        .connectTimeout(cmqConfig.getConnectTimeout() + pollingWaitTime, TimeUnit.MILLISECONDS)
                        .readTimeout(cmqConfig.getReadTimeout() + pollingWaitTime, TimeUnit.MILLISECONDS).build();
                response = okHttpClient.newCall(request).execute();
            } else if(ActionProperties.POLLING.equals(actionProperties.getActionType())){
                //receive单独使用长超时时间的client
                response = receiveHttpClient.newCall(request).execute();
            }else {
                response = httpClient.newCall(request).execute();
            }
            result = response.body().string();
            long duration = System.currentTimeMillis() - start;
            if (cmqConfig.isAlwaysPrintResultLog()) {
                log.info("exec time: {},response:{}", duration, result);
            } else if (cmqConfig.isPrintSlow() && duration > cmqConfig.getSlowThreshold()) {
                log.warn("exec time: {},response:{}", duration, result);
            }
        } catch (Exception e) {
            long duration = System.currentTimeMillis() - start;
            log.error("request fail ,exec time: {}", duration, e);
            throw e;
        }
        return result;
    }

    /**
     * 发送httppost请求
     *
     * @param url
     * @param data 提交的参数为key=value&key1=value1的形式
     * @return
     */
    private static String httpPost(String url, String data, CmqConfig cmqConfig, ActionProperties actionProperties) throws Exception {
        String result;
        RequestBody requestBody = RequestBody.create(MediaType.parse("*/*;charset=utf-8"), data);
        Request request = new Request.Builder().url(url).post(requestBody).build();
        if (log.isDebugEnabled()) {
            log.debug("request:{} timeout:{} data:{}", request.toString(), httpClient.readTimeoutMillis(), data);
        }
        result = doRequest(cmqConfig, request, actionProperties);
        return result;
    }
}
