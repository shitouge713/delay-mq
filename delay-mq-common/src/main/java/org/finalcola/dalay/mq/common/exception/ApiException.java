package org.finalcola.dalay.mq.common.exception;

import javax.annotation.Nonnull;

/**
 * @author: finalcola
 * @date: 2023/3/31 23:15
 */
public class ApiException extends RuntimeException {

    private final int errorCode;
    private final String customMessage;

    public ApiException(int errorCode, String customMessage) {
        super(customMessage);
        this.errorCode = errorCode;
        this.customMessage = customMessage;
    }

    public ApiException(@Nonnull ResultCode resultCode) {
        this(resultCode.getValue(), resultCode.getDesc());
    }

    public ApiException(@Nonnull ResultCode resultCode, String customMessage) {
        this(resultCode.getValue(), customMessage);
    }

    public static ApiException of(@Nonnull ResultCode resultCode) {
        return new ApiException(resultCode);
    }
}
