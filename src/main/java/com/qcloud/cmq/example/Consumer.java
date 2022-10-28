package com.qcloud.cmq.example;

import com.qcloud.cmq.Account;
import com.qcloud.cmq.CMQClientException;
import com.qcloud.cmq.Message;
import com.qcloud.cmq.Queue;

import java.util.ArrayList;
import java.util.List;

public class Consumer {
    public static void main(String[] args) {
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
            //接收单条消息
            System.out.println("---------------receive message ...---------------");
            Message msg = queue.receiveMessage();

            System.out.println("msgId:" + msg.msgId);
            System.out.println("msgBody:" + msg.msgBody);
            System.out.println("receiptHandle:" + msg.receiptHandle);
            System.out.println("enqueueTime:" + msg.enqueueTime);
            System.out.println("nextVisibleTime:" + msg.nextVisibleTime);
            System.out.println("firstDequeueTime:" + msg.firstDequeueTime);
            System.out.println("dequeueCount:" + msg.dequeueCount);

            // 删除消息
            System.out.println("---------------delete message ...---------------");
            queue.deleteMessage(msg.receiptHandle);
            System.out.println("receiptHandle:" + msg.receiptHandle + " deleted");

            //批量接收消息
            ArrayList<String> handles = new ArrayList<String>(); //保存服务器返回的消息句柄，用于删除消息
            System.out.println("---------------batch receive message ...---------------");
            List<Message> messages = queue.batchReceiveMessage(10);
            System.out.println("receive msg count:" + messages.size());
            for (int i = 0; i < messages.size(); i++) {
                msg = messages.get(i);
                System.out.println("msgId:" + msg.msgId);
                System.out.println("msgBody:" + msg.msgBody);
                System.out.println("receiptHandle:" + msg.receiptHandle);
                System.out.println("enqueueTime:" + msg.enqueueTime);
                System.out.println("nextVisibleTime:" + msg.nextVisibleTime);
                System.out.println("firstDequeueTime:" + msg.firstDequeueTime);
                System.out.println("dequeueCount:" + msg.dequeueCount);
                System.out.println();

                handles.add(msg.receiptHandle);
            }

            // 批量删除消息
            System.out.println("---------------batch delete message ...---------------");
            queue.batchDeleteMessage(handles);
            for (int i = 0; i < handles.size(); i++) {
                System.out.println("receiptHandle:" + handles.get(i) + " deleted");
            }
        } catch (CMQClientException e2) {
            System.out.println("Client Exception, " + e2.toString());
        } catch (Exception e) {
            System.out.println("error..." + e.toString());
        }
    }
}
