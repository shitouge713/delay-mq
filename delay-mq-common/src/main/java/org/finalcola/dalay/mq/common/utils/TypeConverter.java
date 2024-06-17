package org.finalcola.dalay.mq.common.utils;

import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.finalcola.dalay.mq.common.collection.MapBuilder;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * @author: finalcola
 * @date: 2023/3/30 22:57
 */
public class TypeConverter {

    private static final Map<Class, Function<String, Object>> CONVERT_FUN_MAP = MapBuilder.<Class, Function<String, Object>>newBuilder()
            .put(Byte.class, NumberUtils::toByte)
            .put(Byte.TYPE, NumberUtils::toByte)
            .put(Boolean.class, Boolean::parseBoolean)
            .put(Boolean.TYPE, Boolean::parseBoolean)
            .put(Character.class, s -> StringUtils.isEmpty(s) ? ' ' : s.toCharArray()[0])
            .put(Character.TYPE, s -> StringUtils.isEmpty(s) ? ' ' : s.toCharArray()[0])
            .put(Short.class, NumberUtils::toShort)
            .put(Short.TYPE, NumberUtils::toShort)
            .put(Integer.class, NumberUtils::toInt)
            .put(Integer.TYPE, NumberUtils::toInt)
            .put(Float.class, NumberUtils::toFloat)
            .put(Float.TYPE, NumberUtils::toFloat)
            .put(Long.class, NumberUtils::toLong)
            .put(Long.TYPE, NumberUtils::toLong)
            .put(Double.class, NumberUtils::toDouble)
            .put(Double.TYPE, NumberUtils::toDouble)
            .put(String.class, s -> s)
            .put(BigDecimal.class, BigDecimal::new)
            .put(BigInteger.class, BigInteger::new)
            .put(BigDecimal.class, BigDecimal::new)
            .put(Date.class, s -> MoreFunctions.runCatching(() -> DateUtils.parseDate(s, "yyyy/MM/dd HH:mm:ss", "yyyy/MM/dd", "yyyyMMdd")))
            .build();

    @Nullable
    public static <T> T convert(@Nonnull Class<T> type, String value) {
        if (type.isEnum()) {
            Enum enumInstance = EnumUtils.getEnum((Class<? extends Enum>) type, value);
            return (T) enumInstance;
        }
        return Optional.ofNullable(CONVERT_FUN_MAP.get(type))
                .map(mapper -> (T) mapper.apply(value))
                .orElse(null);
    }
}
