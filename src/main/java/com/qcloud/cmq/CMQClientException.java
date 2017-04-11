package com.qcloud.cmq;

/**
 * TODO CMQClientException handle all exception caused by client side.
 *
 * @author York.
 *         Created 2016年9月30日.
 */
public class CMQClientException extends RuntimeException {

    /**
     * TODO .
     *
     * @param message
     */
    public CMQClientException(String message) {
        super(message);
    }
}
