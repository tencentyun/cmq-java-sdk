package com.qcloud.cmq;

import java.util.Vector;

/**
 * meta of subscription .
 * createTime and lastModifyTime default set 0
 * you should set createTime and lastModifyTime
 * @author York.
 *         Created 2016年9月26日.
 */
public class SubscriptionMeta {
	final static String notifyStrategyDefault="BACKOFF_RETRY";
	final static String notifyContentFormatDefault="JSON";
	
	//Subscription 订阅的主题所有者的appId
	public String TopicOwner;
	//订阅的终端地址
	public String Endpoint;
	//订阅的协议
	public String Protocal;
	//推送消息出现错误时的重试策略
	public String NotifyStrategy;
	//向 Endpoint 推送的消息内容格式
	public String NotifyContentFormat;
	//描述了该订阅中消息过滤的标签列表（仅标签一致的消息才会被推送）
	public Vector<String> FilterTag;
	//Subscription 的创建时间，从 1970-1-1 00:00:00 到现在的秒值
	public int CreateTime;
	//修改 Subscription 属性信息最近时间，从 1970-1-1 00:00:00 到现在的秒值
	public int LastModifyTime;
	//该订阅待投递的消息数
	public int msgCount;
    public Vector<String>bindingKey;	
	
	
	/**
	 * subscription meta class .
	 *
	 */
	public SubscriptionMeta()
	{
		 TopicOwner ="";
		 Endpoint ="";
		 Protocal ="";
		 NotifyStrategy=notifyStrategyDefault;
		 NotifyContentFormat=notifyContentFormatDefault;
		 FilterTag = null;
		 CreateTime=0;
		 LastModifyTime=0;
		 msgCount=0;
         bindingKey = null;
	}

}
