package com.qcloud.cmq;

public class CMQServerException extends RuntimeException {

	private int httpStatus = 200;
    private int errorCode = 0;
	private String errorMessage = "";

    public CMQServerException(int status){
		this.httpStatus = status;
	}
    public CMQServerException(int errorCode, String errorMessage){
        super(errorMessage);
        this.errorCode = errorCode;
		this.errorMessage = errorMessage;
    }

    @Override
    public String toString() {
	if(httpStatus != 200) {
        return "http status:" + httpStatus;
    } else {
        return "code:" + errorCode
+ ", message:" + errorMessage;
    }
    }
}
