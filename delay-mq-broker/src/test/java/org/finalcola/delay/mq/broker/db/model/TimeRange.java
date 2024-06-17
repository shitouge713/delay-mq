package org.finalcola.delay.mq.broker.db.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.joda.time.DateTime;

import java.time.Duration;

/**
 * @author: finalcola
 * @date: 2023/3/23 22:46
 */
@Data
@AllArgsConstructor
public class TimeRange {

    private final long startTime;
    private final long endTime;

    public static TimeRange withinSeconds(int seconds) {
        DateTime now = DateTime.now();
        return new TimeRange(now.getMillis(), now.plusSeconds(seconds).getMillis());
    }

    public TimeRange minus(Duration duration) {
        long millis = duration.toMillis();
        return new TimeRange(startTime - millis, endTime - millis);
    }

    public TimeRange plus(Duration duration) {
        long millis = duration.toMillis();
        return new TimeRange(startTime + millis, endTime + millis);
    }
}
