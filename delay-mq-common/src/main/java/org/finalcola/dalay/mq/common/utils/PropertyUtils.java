package org.finalcola.dalay.mq.common.utils;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.finalcola.dalay.mq.common.constants.Property;
import org.finalcola.dalay.mq.common.constants.PropertyMapping;
import org.finalcola.dalay.mq.common.exception.ApiException;
import org.finalcola.dalay.mq.common.exception.ResultCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * @author: finalcola
 * @date: 2023/3/29 23:22
 */
public class PropertyUtils {
    private static final Logger logger = LoggerFactory.getLogger(PropertyUtils.class);

    private static final Map<Class, List<Field>> FIELD_CACHE = new ConcurrentHashMap<>();

    @Nullable
    public static <T> T readPropertiesConfig(@Nonnull Class<T> klass) {
        Properties properties = readProperties(klass);
        if (properties == null) {
            logger.warn("readPropertiesConfig({}) fail, file not found", klass.getName());
            return null;
        }

        try {
            T instance = klass.getDeclaredConstructor().newInstance();
            List<Field> fields = getFields(klass);
            for (Field field : fields) {
                Property property = field.getAnnotation(Property.class);
                String key = Optional.ofNullable(property)
                        .map(Property::value)
                        .filter(StringUtils::isNotBlank)
                        .orElseGet(field::getName);
                String value = resolveValue(key, properties, property == null ? null : property.defaultValue());
                if (value == null && property != null && property.required()) {
                    throw new ApiException(ResultCode.CONFIG_ERROR, "miss config:" + key);
                }

                Object convertedValue = TypeConverter.convert(field.getType(), value);
                // TODO: 2023/3/31 递归构建
                field.set(instance, convertedValue);
            }
            return instance;
        } catch (ReflectiveOperationException e) {
            logger.error("read config error,config class:{}", klass.getName(), e);
            return null;
        }
    }

    public static String resolveValue(String key, Properties properties, String defaultValue) {
        if (StringUtils.isBlank(key)) {
            return "";
        }
        List<Function<String, String>> valueGetters = new ArrayList<>();
        valueGetters.add(properties::getProperty); // 优先从配置文件中取
        valueGetters.add(PropertyUtils::getValueFromEnvironment); // 其次从环境变量中取
        valueGetters.add(k -> defaultValue); // 都没有则取注解上的默认值

        return valueGetters.stream()
                .map(getter -> getter.apply(key))
                .filter(Objects::nonNull)
                .findFirst().orElse(null);
    }

    @Nullable
    private static String getValueFromEnvironment(String key) {
        return Optional.ofNullable(System.getProperty(key))
                .orElseGet(() -> System.getenv(key));
    }

    @Nullable
    public static Properties readProperties(@Nonnull Class<?> klass) {
        return Optional.ofNullable(klass.getAnnotation(PropertyMapping.class))
                .map(PropertyMapping::value)
                .filter(StringUtils::isNotBlank)
                .map(String::trim)
                .map(PropertyUtils::readResourceProperties)
                .orElse(null);
    }

    @Nullable
    public static Properties readResourceProperties(@Nonnull String filePath) {
        String content = readResourceFile(filePath);
        if (StringUtils.isEmpty(content)) {
            return new Properties();
        }
        Properties properties = new Properties();
        String[] lines = content.split("\n");
        Arrays.stream(lines)
                .map(StringUtils::trimToEmpty)
                .map(s -> {
                    int index = s.indexOf("=");
                    if (index == -1) {
                        return null;
                    }
                    String key = StringUtils.trimToEmpty(s.substring(0, index));
                    String value = index == s.length() - 1 ? "" : StringUtils.trimToEmpty(s.substring(index + 1));
                    return Pair.of(key, value);
                })
                .filter(Objects::nonNull)
                .filter(pair -> StringUtils.isNotEmpty(pair.getKey()))
                .forEach(pair -> properties.setProperty(pair.getKey(), pair.getValue()));
        return properties;
    }

    @Nullable
    public static String readResourceFile(@Nonnull String filePath) {
        URL fileUrl = Thread.currentThread().getContextClassLoader().getResource(filePath);
        if (fileUrl == null) {
            return null;
        }
        try {
            return new String(Files.readAllBytes(Paths.get(fileUrl.toURI())), StandardCharsets.UTF_8);
        } catch (Exception e) {
            logger.error("read resource({}) error", fileUrl, e);
            return null;
        }
    }

    private static List<Field> getFields(@Nonnull Class klass) {
        return FIELD_CACHE.computeIfAbsent(klass, k -> {
            List<Field> fields = FieldUtils.getFieldsListWithAnnotation(klass, Property.class);
            fields.forEach(field -> field.setAccessible(true));
            return fields;
        });
    }
}
