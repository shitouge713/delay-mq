package org.finalcola.delay.mq.broker.config;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author: finalcola
 * @date: 2023/3/18 23:53
 */
public class ExecutorDef {
    private static final int PROCESSOR_COUNT = Runtime.getRuntime().availableProcessors();

    public static final ScheduledExecutorService SCHEDULER = Executors.newScheduledThreadPool(PROCESSOR_COUNT, new ThreadFactoryBuilder()
            .setNameFormat("delay-broker-scheduler")
            .build());

    public static final ExecutorService MSG_INPUT_EXECUTOR = Executors.newFixedThreadPool(PROCESSOR_COUNT * 2, new ThreadFactoryBuilder()
            .setNameFormat("delay-msg-in-thread")
            .build());

    public static final ExecutorService MSG_OUTPUT_EXECUTOR = Executors.newFixedThreadPool(PROCESSOR_COUNT * 2, new ThreadFactoryBuilder()
            .setNameFormat("delay-msg-out-thread")
            .build());

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            SCHEDULER.shutdown();
            MSG_INPUT_EXECUTOR.shutdown();
            MSG_OUTPUT_EXECUTOR.shutdown();
        }));
    }

}
