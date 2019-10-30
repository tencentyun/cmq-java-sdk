package com.qcloud.cmq;

import lombok.extern.slf4j.Slf4j;
import okhttp3.*;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
@Slf4j
public class HttpUtil {
    public static int timeout = 1000;
    public static String request(String method, String url, String data, int userTimeout) throws Exception{
        String result = "";
        if (method.equals("POST")) {
            result = httpPost(url,data,userTimeout);
        }
        else{
            result = httpGet(url, userTimeout);
        }
        return result;
    }


    private static String httpGet(String url, int userTimeout) throws Exception{
        String result = null;
        OkHttpClient client = new OkHttpClient().newBuilder().connectTimeout(userTimeout + timeout, TimeUnit.MILLISECONDS).readTimeout(userTimeout + timeout, TimeUnit.MILLISECONDS).build();
        Request request = new Request.Builder().url(url).build();
        long start = System.currentTimeMillis();
        log.debug("request:{} timeout:{}",request.toString(),client.readTimeoutMillis());
        try {
            Response response = client.newCall(request).execute();
            long interval = System.currentTimeMillis() - start;
            result = response.body().string();
            if(log.isDebugEnabled()) {
                log.debug("exec time: {},response:{},request:{}", interval, result, url);
            }else if(log.isInfoEnabled()){
                log.info("exec time: {},response:{}", interval, result);
            }else if(interval > 1000){
                log.warn("exec time: {},response:{},request:{}", interval, result,url);
            }else if(interval > 3000){
                log.error("exec time: {},response:{},request:{}", interval, result,url);
            }
        } catch (Exception e) {
            long interval = System.currentTimeMillis() - start;
            log.error("exec time: {},request:{},err:{}", interval,url,e.getMessage());
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
    private static String httpPost(String url, String data, int userTimeout) throws Exception {
        String result = null;
        OkHttpClient httpClient = new OkHttpClient().newBuilder().connectTimeout(userTimeout + timeout, TimeUnit.MILLISECONDS).readTimeout(userTimeout + timeout, TimeUnit.MILLISECONDS).build();
        RequestBody requestBody = RequestBody.create(MediaType.parse("*/*;charset=utf-8"), data);
        Request request = new Request.Builder().url(url).post(requestBody).build();
        long start = System.currentTimeMillis();
        log.debug("request:{} timeout:{} data:{}",request.toString(), httpClient.readTimeoutMillis(),data);

        try {
            Response response = httpClient.newCall(request).execute();
            long interval = System.currentTimeMillis() - start;
            result = response.body().string();
            if(log.isDebugEnabled()) {
                log.debug("exec time: {},response:{},request:{}",interval,result,data);
            }else if(log.isInfoEnabled()){
                log.info("exec time: {},response:{}", interval, result);
            }else if(interval > 1000){
                log.warn("exec time: {},response:{},request:{}", interval, result,data);
            }else if(interval > 3000){
                log.error("exec time: {},response:{},request:{}", interval, result,data);
            }
        } catch (Exception e) {
            long interval = System.currentTimeMillis() - start;
            log.error("exec time: {},request:{},err:{}", interval,url,e.getMessage());
            throw e;
        }
        return result;
    }
}
