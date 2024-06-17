package org.finalcola.dalay.mq.common.utils;

import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.vavr.CheckedRunnable;

import java.util.concurrent.Callable;

/**
 * @author: finalcola
 * @date: 2023/3/19 00:20
 */
public class RetryUtils {

    public static void retry(int maxTime, CheckedRunnable runnable) {
        RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(maxTime)
                .failAfterMaxAttempts(true)
                .build();
        Retry retry = Retry.of("common", retryConfig);
        Retry.decorateCheckedRunnable(retry, runnable)
                .unchecked()
                .run();
    }

    public static <T> T retry(int maxTime, Callable<T> callable) {
        RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(maxTime)
                .failAfterMaxAttempts(true)
                .build();
        Retry retry = Retry.of("common", retryConfig);
        try {
            return Retry.decorateCallable(retry, callable)
                    .call();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
