package org.finalcola.dalay.mq.common.exception;

/**
 * @author: shanshan
 * @date: 2023/3/31 23:22
 */
public enum ResultCode {
    OP_FAIL(1, "操作失败"),
    CONFIG_ERROR(2, "配置错误"),
    ;
    private final int value;
    private final String desc;

    ResultCode(int value, String desc) {
        this.value = value;
        this.desc = desc;
    }

    public int getValue() {
        return value;
    }

    public String getDesc() {
        return desc;
    }
}
