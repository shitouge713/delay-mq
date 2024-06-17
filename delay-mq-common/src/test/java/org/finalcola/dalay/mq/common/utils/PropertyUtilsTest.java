package org.finalcola.dalay.mq.common.utils;


import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * @author: shanshan
 * @date: 2023/3/29 23:54
 */
public class PropertyUtilsTest {
    private static final Logger logger = LoggerFactory.getLogger(PropertyUtilsTest.class);
    private final String filePath = "test.properties";

    @Test
    public void readResourceFile() {
        String content = PropertyUtils.readResourceFile(filePath);
        logger.info("content:{}", content);
        Assert.assertTrue(StringUtils.isNotEmpty(content));
    }

    @Test
    public void readPropertiesTest() {
        Properties properties = PropertyUtils.readResourceProperties(filePath);
        logger.info("properties:{}", properties);
        Assert.assertNotNull(properties);
        Assert.assertEquals(properties.getProperty("k1"), "v1");
    }
}