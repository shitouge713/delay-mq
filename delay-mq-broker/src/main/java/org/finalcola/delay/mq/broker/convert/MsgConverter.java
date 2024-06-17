package org.finalcola.delay.mq.broker.convert;

import com.google.common.base.Joiner;
import org.apache.commons.lang3.StringUtils;
import org.finalcola.delay.mq.common.proto.DelayMsg;
import org.finalcola.delay.mq.common.proto.MetaData;
import org.finalcola.delay.mq.common.proto.MsgDataWrapper;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * @author: finalcola
 * @date: 2023/3/18 13:03
 */
public class MsgConverter {

    @Nonnull
    public static ByteBuffer toByteBuffer(String str) {
        if (StringUtils.isEmpty(str)) {
            return ByteBuffer.wrap(new byte[0]);
        }
        return ByteBuffer.wrap(str.getBytes(UTF_8));
    }

    @Nullable
    public static String toString(ByteBuffer byteBuffer) {
        if (byteBuffer == null) {
            return null;
        }
        return new String(byteBuffer.array(), UTF_8);
    }

    public static ByteBuffer buildKey(@Nonnull DelayMsg msg) {
        MetaData metaData = msg.getMetaData();
        long expireTime = metaData.getDelayMills() + metaData.getCreateTimestamp();
        String keyStr = Joiner.on("|")
                .join(expireTime, metaData.getTopic(), metaData.getMsgId());
        return toByteBuffer(keyStr);
    }

    public static DelayMsg buildDelayMsg(String msgId, MsgDataWrapper wrapper) {
        MetaData metaData = MetaData.newBuilder()
                .setMsgId(msgId)
                .setMsgKey(wrapper.getMsgKey())
                .setCreateTimestamp(wrapper.getCreateTime())
                .setDelayMills(wrapper.getDelayMills())
                .setTopic(wrapper.getTopic())
                .setTags(wrapper.getTags())
                .setPartitionKey(wrapper.getPartitionKey())
                .build();
        return DelayMsg.newBuilder()
                .setMetaData(metaData)
                .setBody(wrapper.getData())
                .build();
    }
}
