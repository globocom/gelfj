package org.graylog2.sender;

/**
 * Created by lucas.castro on 10/7/16.
 */
public class GelfTimeoutException extends GelfSenderException {


    public GelfTimeoutException(int errorCode, String message, Throwable cause) {
        super(errorCode, message, cause);
    }

    public GelfTimeoutException(int errorCode, String message) {
        super(errorCode, message);
    }
}
