package com.qcloud.cmq.example;

import com.qcloud.cmq.Account;
import com.qcloud.cmq.CMQServerException;
import com.qcloud.cmq.Queue;
import com.qcloud.cmq.QueueMeta;

import java.util.ArrayList;
import java.util.List;

public class Producer {
    public static void main(String[] args){
        //从腾讯云官网查询的云API密钥信息
        String secretId="AKIDlRs3zfTGBqKZYIF35rOhmbAE3dx1LZzF";
        String secretKey="DBuNaaLq55pLbcS3QZS6GAoGSO4VgbGn";
        String endpoint = "https://cmq-queue-gz.api.qcloud.com";

        Account account = new Account(endpoint,secretId, secretKey);

        try {

//            创建新队列

//            System.out.println("---------------create queue ...---------------");
//            String queueName = "name";
//            QueueMeta meta = new QueueMeta();
//            meta.pollingWaitSeconds = 10;
//            meta.visibilityTimeout = 10;
//            meta.maxMsgSize = 1048576;
//            meta.msgRetentionSeconds = 345600;
//            Queue queue = account.createQueue(queueName,meta);
//            System.out.println(queueName + " created");

//            列出当前帐号下所有队列名字

//            System.out.println("---------------list queue ...---------------");
//            ArrayList<String> vtQueue = new ArrayList<String>();
//            int totalCount = account.listQueue("",-1,-1,vtQueue);
//            System.out.println("totalCount:" + totalCount);
//            for(int i=0;i<vtQueue.size();i++)
//            {
//                System.out.println("queueName:" + vtQueue.get(i));
//            }

//            删除队列

//            System.out.println("---------------delete queue ...---------------");
//            account.deleteQueue(queueName);
//            System.out.println(queueName + " deleted");

            // 获得队列实例（此处直接使用现有队列进行操作，也可按照上面的注释创建队列）
            System.out.println("--------------- queue[qiyuan-test] ---------------");
            String queueName = "qiyuan-test";
            Queue queue = account.getQueue(queueName);

            // 设置队列属性
            System.out.println("---------------set queue attributes ...---------------");
            QueueMeta meta1 = new QueueMeta();
            meta1.pollingWaitSeconds = 20;
            queue.setQueueAttributes(meta1);
            System.out.println("pollingWaitSeconds=20 set");

            // 获取队列属性
            System.out.println("---------------get queue attributes ...---------------");
            QueueMeta meta2 = queue.getQueueAttributes();
            System.out.println("maxMsgHeapNum:" + meta2.maxMsgHeapNum);
            System.out.println("pollingWaitSeconds:" + meta2.pollingWaitSeconds);
            System.out.println("visibilityTimeout:" + meta2.visibilityTimeout);
            System.out.println("maxMsgSize:" + meta2.maxMsgSize);
            System.out.println("createTime:" + meta2.createTime);
            System.out.println("lastModifyTime:" + meta2.lastModifyTime);
            System.out.println("activeMsgNum:" + meta2.activeMsgNum);
            System.out.println("inactiveMsgNum:" + meta2.inactiveMsgNum);

            // 发送单条信息
            System.out.println("---------------send message ...---------------");
            String msg = "hello!";
            String msgId = queue.sendMessage(msg);
            System.out.println("==> send success! msg_id:" + msgId);

            //批量操作
            //批量发送消息
            System.out.println("---------------batch send message ...---------------");
            ArrayList<String> vtMsgBody = new ArrayList<String>();
            String msgBody = "hello world,this is cmq sdk for java 1";
            vtMsgBody.add(msgBody);
            msgBody = "hello world,this is cmq sdk for java 2";
            vtMsgBody.add(msgBody);
            msgBody = "hello world,this is cmq sdk for java 3";
            vtMsgBody.add(msgBody);
            List<String> vtMsgId = queue.batchSendMessage(vtMsgBody);
            for(int i=0;i<vtMsgBody.size();i++)
                System.out.println("[" + vtMsgBody.get(i) + "] sent");
            for(int i=0;i<vtMsgId.size();i++)
                System.out.println("msgId:" + vtMsgId.get(i));


        }catch(CMQServerException e1){
            System.out.println("Server Exception, " + e1.toString());
        }
        catch (Exception e) {
            System.out.println("error..." + e.toString());
        }
    }
}
