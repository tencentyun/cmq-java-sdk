package com.qcloud.cmq.example;

import com.qcloud.cmq.*;

import java.util.ArrayList;
import java.util.Vector;

public class TopicDemo {
    public static void main(String[] args) {
        // 请在腾讯云官网查看 id ,key endpoint
        String secretId = "";
        String secretKey = "";
        String token = ""; // for auth with temporary secret
        String endpoint = "https://cmq-gz.public.tencenttdmq.com";
        String topicName = "test-1";
        try {
            // 获取 account
            System.out.println("---------------- init account ----------------");
            //Account account = new Account(endpoint, secretId, secretKey);
            Account account = new Account(endpoint, secretId, secretKey, token);

            System.out.println("---------------- get topic ----------------");
            Topic topic = account.getTopic(topicName);

            //发布信息
            System.out.println("---------------- publish message ----------------");
            String msg = "hello!";
            String msgId = topic.publishMessage(msg);
            System.out.println("msgId: " + msgId);

            // 批量发布信息
            System.out.println("---------------- batch publish message ----------------");
            Vector<String> messages = new Vector<String>();
            int batchMessageSize = 5;
            for (int i = 0; i < batchMessageSize; ++i) {
                msg = "this is a test message publish with index = " + i;
                messages.add(msg);
            }
            Vector<String> msgIds = topic.batchPublishMessage(messages);
            System.out.println("msgIds: " + msgIds.toString());
        } catch (CMQServerException e1) {
            System.out.println("Server Exception, " + e1.toString());
        } catch (CMQClientException e2) {
            System.out.println("Client Exception, " + e2.toString());
        } catch (Exception e) {
            System.out.println("error..." + e.toString());
        }
    }
}
