package org.finalcola.dalay.mq.common.exception;

import com.fasterxml.jackson.core.JsonProcessingException;

import java.io.UncheckedIOException;

/**
 * @author: finalcola
 * @date: 2023/3/16 22:33
 */
public class UncheckedJsonException extends UncheckedIOException {

    public UncheckedJsonException(JsonProcessingException cause) {
        super(cause.getMessage(), cause);
    }
}
