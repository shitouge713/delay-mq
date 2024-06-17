package org.finalcola.delay.mq.broker.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.finalcola.delay.mq.common.proto.DelayMsg;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

/**
 * @author: finalcola
 * @date: 2023/3/18 21:34
 */
@Data
@AllArgsConstructor
public class ScanResult {
    @Nullable
    private final String lastMsgStoreKey;
    @Nonnull
    private final List<DelayMsg> delayMsgs;
}
