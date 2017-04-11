package com.qcloud.cmq;

/**
 * topic meta class .
 * TopicMeta class 
 * createTime and lastModifyTime default set 0
 * you should set createTime and lastModifyTime
 * @author York.
 *         Created 2016年9月26日.
 */
public class TopicMeta {
	// 当前该主题的消息堆积数
	public  int msgCount;
	// 消息最大长度，取值范围1024-65536 Byte（即1-64K），默认65536
	public  int maxMsgSize;
	//消息在主题中最长存活时间，从发送到该主题开始经过此参数指定的时间后，
	//不论消息是否被成功推送给用户都将被删除，单位为秒。固定为一天，该属性不能修改。
    public  int msgRetentionSeconds;
    //创建时间
	public  int createTime;
	//修改属性信息最近时间
	public  int lastModifyTime;
	public  int loggingEnabled;
    public  int filterType ;
	
	public TopicMeta()
	{
		msgCount = 0;
		maxMsgSize= 65536;
		msgRetentionSeconds = 86400;
		createTime = 0;
		lastModifyTime = 0;
		loggingEnabled = 0;
        filterType = 1 ;
	}
        	 
}
