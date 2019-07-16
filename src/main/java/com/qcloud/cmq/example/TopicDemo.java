package com.qcloud.cmq.example;

import com.qcloud.cmq.*;

import java.util.ArrayList;
import java.util.Vector;

public class TopicDemo {
    public static void main(String[] args){
        // 请在腾讯云官网查看 id ,key endpoint
        String secretId="";
        String secretKey="";
        String endpoint = "https://cmq-topic-{$region}.api.qcloud.com";
        try {
            int batchMessageSize = 5 ;

            // 获取 account
            System.out.println("---------------- init account ----------------");
            Account account = new Account(endpoint,secretId, secretKey);


//            创建 topic
//            System.out.println("---------------- create topic ----------------");
//            String topicName="topic-test1";
//            account.createTopic(topicName, 1024*1024);

            // 获取 topic
            System.out.println("---------------- get topic ----------------");
            String topicName = "qiyuan-test";
            Topic topic = account.getTopic(topicName);
            Thread.sleep(1000);

            // 设置和获取 topic 元数据
            System.out.println("---------------- set and get topic meta ----------------");
            TopicMeta topicMeta = new TopicMeta();
            topicMeta.maxMsgSize = 32768;
            topic.setTopicAttributes(topicMeta.maxMsgSize);
            topicMeta = topic.getTopicAttributes();
            System.out.println("maxMsgSize: "+ topicMeta.maxMsgSize);
            System.out.println("createTime: "+ topicMeta.createTime);
            System.out.println("lastModifyTime: "+ topicMeta.lastModifyTime);
            System.out.println("msgCount: "+ topicMeta.msgCount);

            // 创建 sub
//            System.out.println("---------------- create sub ----------------");
//            String queueName = "test";
//            String subscriptionName = "sub-test";
//            String Endpoint = queueName;
//            String Protocol = "queue";
//            account.createSubscribe(topicName,subscriptionName, Endpoint, Protocol);

            // 获取 sub
            System.out.println("---------------- get sub ----------------");
            String subscriptionName = "qiyuan-test-sub";
            Subscription sub = account.getSubscription(topicName,subscriptionName);

            // 获取 Subscription 元数据
            SubscriptionMeta subscriptionMeta = sub.getSubscriptionAttributes();
            System.out.println("Endpoint: " + subscriptionMeta.Endpoint);
            System.out.println("Protocal " + subscriptionMeta.Protocal);
            System.out.println("TopicOwner: " + subscriptionMeta.TopicOwner);
            System.out.println("CreateTime: " + subscriptionMeta.CreateTime);
            System.out.println("msgCount: " + subscriptionMeta.msgCount);

            // 列出当前 topic 下所有 sub
            System.out.println("---------------- list sub ----------------");
            ArrayList< String> vSubscription = new ArrayList<String>();

            int SubscriptionCount = topic.ListSubscription(-1,-1,"",vSubscription);
            for (String subscription : vSubscription)
            {
                System.out.println("Subscription name :" + subscription);
            }

            //发布信息
            System.out.println("---------------- publish message ----------------");
            String msg = "hello!";
            String msgId = topic.publishMessage(msg);
            System.out.println("msgId: " + msgId);

            // 批量发布信息
            System.out.println("---------------- batch publish message ----------------");
            Vector<String> vMsg = new Vector<String>();
            for(int i = 0 ; i< batchMessageSize ; ++ i )
            {
                String msgItem ="this is a test message publish with index = " + i;
                vMsg.add(msgItem);
            }
            Vector<String> msgIds = topic.batchPublishMessage(vMsg);
            System.out.println("msgIds: " + msgIds.toString());

//            删除 sub 和 topic

//            account.deleteSubscribe(topicName,subscriptionName);
//            System.out.println("---------------- delete sub ----------------");
//            account.deleteTopic(topicName);
//            System.out.println("---------------- delete topic ----------------");

        }catch(CMQServerException e1){
            System.out.println("Server Exception, " + e1.toString());
        } catch(CMQClientException e2){
            System.out.println("Client Exception, " + e2.toString());
        } catch (Exception e) {
            System.out.println("error..." + e.toString());
        }
    }
}
