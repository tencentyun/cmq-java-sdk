package com.qcloud.cmq.example;

import com.qcloud.cmq.Account;
import com.qcloud.cmq.CMQClientException;
import com.qcloud.cmq.Message;
import com.qcloud.cmq.Queue;

import java.util.ArrayList;
import java.util.List;

public class Consumer {
    public static void main(String[] args){
        String secretId="";
        String secretKey="";
        String endpoint = "https://cmq-queue-{$region}.api.qcloud.com";
        String queueName = "test";

        Account account = new Account(endpoint,secretId, secretKey);
        //获得队列实例
        System.out.println("--------------- queue[qiyuan-test] ---------------");
        Queue queue = account.getQueue(queueName);
        try{
            //接收单条消息
            System.out.println("---------------recv message ...---------------");
            Message msg = queue.receiveMessage(10);

            System.out.println("msgId:" + msg.msgId);
            System.out.println("msgBody:" + msg.msgBody);
            System.out.println("receiptHandle:" + msg.receiptHandle);
            System.out.println("enqueueTime:" + msg.enqueueTime);
            System.out.println("nextVisibleTime:" + msg.nextVisibleTime);
            System.out.println("firstDequeueTime:" + msg.firstDequeueTime);
            System.out.println("dequeueCount:" + msg.dequeueCount);

//            删除消息
//            System.out.println("---------------delete message ...---------------");
//            queue.deleteMessage(msg.receiptHandle);
//            System.out.println("receiptHandle:" + msg.receiptHandle +" deleted");

            //批量接收消息
            ArrayList<String> vtReceiptHandle = new ArrayList<String>(); //保存服务器返回的消息句柄，用于删除消息
            System.out.println("---------------batch recv message ...---------------");
            List<Message> msgList = queue.batchReceiveMessage(10,10);
            System.out.println("recv msg count:" + msgList.size());
            for(int i=0;i<msgList.size();i++)
            {
                Message msg1 = msgList.get(i);
                System.out.println("msgId:" + msg1.msgId);
                System.out.println("msgBody:" + msg1.msgBody);
                System.out.println("receiptHandle:" + msg1.receiptHandle);
                System.out.println("enqueueTime:" + msg1.enqueueTime);
                System.out.println("nextVisibleTime:" + msg1.nextVisibleTime);
                System.out.println("firstDequeueTime:" + msg1.firstDequeueTime);
                System.out.println("dequeueCount:" + msg1.dequeueCount);
                System.out.println();

                vtReceiptHandle.add(msg1.receiptHandle);
            }

//            批量删除消息
//            System.out.println("---------------batch delete message ...---------------");
//            queue.batchDeleteMessage(vtReceiptHandle);
//            for(int i=0;i<vtReceiptHandle.size();i++)
//                System.out.println("receiptHandle:" + vtReceiptHandle.get(i) + " deleted");
        }catch(CMQClientException e2){
            System.out.println("Client Exception, " + e2.toString());
        }catch (Exception e) {
            System.out.println("error..." + e.toString());
        }
    }
}
