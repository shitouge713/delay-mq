package org.finalcola.dalay.mq.common.utils;

import io.vavr.CheckedRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.concurrent.Callable;

/**
 * @author: finalcola
 * @date: 2023/3/19 19:38
 */
public class MoreFunctions {
    private static final Logger logger = LoggerFactory.getLogger(MoreFunctions.class);

    public static void runCatching(@Nonnull CheckedRunnable runnable) {
        try {
            runnable.run();
        } catch (Throwable e) {
            logger.info("[fail safe]", e);
        }
    }

    public static <T> T runCatching(@Nonnull Callable<T> callable) {
        return runCatching(callable, null);
    }

    public static <T> T runCatching(@Nonnull Callable<T> callable, T defaultValue) {
        try {
            return callable.call();
        } catch (Exception e) {
            logger.info("[fail safe]", e);
            return defaultValue;
        }
    }
}
