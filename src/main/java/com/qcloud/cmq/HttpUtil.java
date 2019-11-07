package com.qcloud.cmq;

import com.qcloud.cmq.entity.CmqConfig;
import okhttp3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class HttpUtil {
    private static final Logger log = LoggerFactory.getLogger(HttpUtil.class);

    private static volatile OkHttpClient httpClient;

    public static String request(String url, String data, CmqConfig cmqConfig) throws Exception {
        initHttpClient(cmqConfig);
        String result = "";
        if ("POST".equals(cmqConfig.getMethod())) {
            result = httpPost(url, data, cmqConfig);
        } else {
            result = httpGet(url, cmqConfig);
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
    }

    private static String httpGet(String url, CmqConfig cmqConfig) throws Exception {
        String result = null;
        Request request = new Request.Builder().url(url).build();
        long start = System.currentTimeMillis();
        log.debug("request:{} timeout:{}", request.toString(), httpClient.readTimeoutMillis());
        try {
            result = doRequest(cmqConfig, request);
        } catch (Exception e) {
            long duration = System.currentTimeMillis() - start;
            log.error("exec time: {},request:{},err:{}", duration, url, e.getMessage());
            throw e;
        }
        return result;
    }

    private static String doRequest(CmqConfig cmqConfig, Request request) throws IOException {
        String result;
        long start = System.currentTimeMillis();
        Response response = null;
        if(cmqConfig.isReceive()){
            OkHttpClient okHttpClient = new OkHttpClient().newBuilder()
                    .connectionPool(new ConnectionPool(0, 1L, TimeUnit.MINUTES))
                    .connectTimeout(cmqConfig.getConnectTimeout()+ cmqConfig.getPollingWaitTimeout(), TimeUnit.MILLISECONDS)
                    .readTimeout(cmqConfig.getReadTimeout() + cmqConfig.getPollingWaitTimeout(), TimeUnit.MILLISECONDS).build();
            response = okHttpClient.newCall(request).execute();
            //释放线程池
            okHttpClient.connectionPool().evictAll();
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
        return result;
    }

    /**
     * 发送httppost请求
     *
     * @param url
     * @param data 提交的参数为key=value&key1=value1的形式
     * @return
     */
    private static String httpPost(String url, String data, CmqConfig cmqConfig) throws Exception {
        String result;
        RequestBody requestBody = RequestBody.create(MediaType.parse("*/*;charset=utf-8"), data);
        Request request = new Request.Builder().url(url).post(requestBody).build();
        long start = System.currentTimeMillis();
        log.debug("request:{} timeout:{} data:{}", request.toString(), httpClient.readTimeoutMillis(), data);
        try {
            result = doRequest(cmqConfig, request);
        } catch (Exception e) {
            long duration = System.currentTimeMillis() - start;
            log.error("exec time: {},request:{}", duration, url, e);
            throw e;
        }
        return result;
    }
}
