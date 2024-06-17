package org.finalcola.dalay.mq.common.constants;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author: shanshan
 * @date: 2023/3/29 23:17
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
public @interface Property {
    String value() default "";

    String desc();

    boolean required() default false;

    String defaultValue() default "";
}
