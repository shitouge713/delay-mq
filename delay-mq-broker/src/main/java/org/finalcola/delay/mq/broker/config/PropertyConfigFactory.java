package org.finalcola.delay.mq.broker.config;

import org.finalcola.dalay.mq.common.constants.PropertyMapping;
import org.finalcola.dalay.mq.common.exception.ApiException;
import org.finalcola.dalay.mq.common.exception.ResultCode;
import org.finalcola.dalay.mq.common.utils.PropertyUtils;

import javax.annotation.Nonnull;

/**
 * @author: finalcola
 * @date: 2023/3/31 23:13
 */
public class PropertyConfigFactory {

    public static <T> T createConfig(@Nonnull Class<T> configClass) {
        if (configClass.getAnnotation(PropertyMapping.class) == null) {
            throw new ApiException(ResultCode.CONFIG_ERROR);
        }
        return PropertyUtils.readPropertiesConfig(configClass);
    }
}
