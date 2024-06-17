package org.finalcola.dalay.mq.common.utils;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonFactoryBuilder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import org.apache.commons.lang3.StringUtils;
import org.finalcola.dalay.mq.common.exception.UncheckedJsonException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.text.SimpleDateFormat;
import java.util.SimpleTimeZone;

/**
 * @author: finalcola
 * @date: 2023/3/16 22:29
 */
public class JsonUtils {
    public static final ObjectMapper MAPPER = new ObjectMapper(new JsonFactoryBuilder()
            .disable(JsonFactory.Feature.INTERN_FIELD_NAMES)
            .build());

    static {
        MAPPER.registerModule(new GuavaModule());
        MAPPER.registerModule(new ParameterNamesModule());
        // 时区序列化为 +08:00 形式
        MAPPER.disable(SerializationFeature.WRITE_DATES_WITH_ZONE_ID)
                // 日期、时间序列化为字符串
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                // 持续时间序列化为字符串
                .disable(SerializationFeature.WRITE_DURATIONS_AS_TIMESTAMPS)
                // 当出现 Java 类中未知的属性时不报错，而是忽略此 JSON 字段
                .disable(SerializationFeature.FAIL_ON_UNWRAPPED_TYPE_IDENTIFIERS)
                .disable(JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS.mappedFeature())
                // 枚举类型调用 `toString` 方法进行序列化
                .enable(SerializationFeature.WRITE_ENUMS_USING_TO_STRING)
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .enable(JsonReadFeature.ALLOW_JAVA_COMMENTS.mappedFeature())
                // 设置 java.util.Date 类型序列化格式
                .setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"))
                // 设置 Jackson 使用的时区
                .setTimeZone(SimpleTimeZone.getTimeZone("GMT+8"));
        // jackson-module-kotlin
        //        MAPPER.registerModule(new KotlinModule());
        // jackson-datatype-protobuf
        //        MAPPER.registerModule(new ProtobufModule());
    }

    public static String toJson(@Nullable Object obj) {
        try {
            return MAPPER.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw wrap(e);
        }
    }

    public static <T> T fromJson(@Nullable String jsonStr, @Nonnull Class<T> klass) {
        if (StringUtils.isEmpty(jsonStr)) {
            return null;
        }
        try {
            return MAPPER.readValue(jsonStr, klass);
        } catch (IOException e) {
            throw wrap(e);
        }
    }

    public static <T> T fromJson(@Nonnull String jsonStr, @Nonnull TypeReference<T> typeReference) {
        if (StringUtils.isEmpty(jsonStr)) {
            return null;
        }
        try {
            return MAPPER.readValue(jsonStr, typeReference);
        } catch (JsonProcessingException e) {
            throw wrap(e);
        }
    }

    private static RuntimeException wrap(IOException e) {
        if (e instanceof JsonProcessingException) {
            return new UncheckedJsonException((JsonProcessingException) e);
        }
        return new UncheckedIOException(e);
    }
}
