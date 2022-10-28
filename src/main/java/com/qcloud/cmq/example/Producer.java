package com.qcloud.cmq.example;

import com.qcloud.cmq.Account;
import com.qcloud.cmq.CMQServerException;
import com.qcloud.cmq.Queue;
import com.qcloud.cmq.QueueMeta;
import com.qcloud.cmq.entity.CmqResponse;

import java.util.ArrayList;
import java.util.List;

public class Producer {
    public static void main(String[] args) {
        //从腾讯云官网查询的云API密钥信息
        String secretId = "";
        String secretKey = "";
        String token = ""; // for auth with temporary secret
        String endpoint = "https://cmq-gz.public.tencenttdmq.com";
        String queueName = "test-1";

        //Account account = new Account(endpoint, secretId, secretKey);
        Account account = new Account(endpoint, secretId, secretKey, token);

        System.out.printf("--------------- queue[%s] ---------------\n", queueName);
        Queue queue = account.getQueue(queueName);
        try {
            // 发送单条信息
            System.out.println("---------------send message ...---------------");
            String msg = "hello!";
            CmqResponse resp = queue.send(msg);
            System.out.println("==> send success! msg_id:" + resp.getMsgId() + " requestId:" + resp.getRequestId());

            //批量发送消息
            System.out.println("---------------batch send message ...---------------");
            ArrayList<String> messages = new ArrayList<String>();
            msg = "hello world,this is cmq sdk for java 1";
            messages.add(msg);
            msg = "hello world,this is cmq sdk for java 2";
            messages.add(msg);
            msg = "hello world,this is cmq sdk for java 3";
            messages.add(msg);
            List<CmqResponse> responses = queue.batchSend(messages);
            for (int i = 0; i < messages.size(); i++) {
                System.out.println("[" + messages.get(i) + "] sent");
            }
            for (int i = 0; i < responses.size(); i++) {
                CmqResponse response = responses.get(i);
                System.out.println("msgId:" + response.getMsgId() + " requestId:" + response.getRequestId());
            }
        } catch (CMQServerException e1) {
            System.out.println("Server Exception, " + e1.toString());
        } catch (Exception e) {
            System.out.println("error..." + e.toString());
        }
    }
}
