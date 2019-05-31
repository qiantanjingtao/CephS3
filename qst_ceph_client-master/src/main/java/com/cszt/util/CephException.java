package com.cszt.util;

public class CephException extends Exception {

    private static final long serialVersionUID = 1L;
    private Integer code;
    private String msg;

    public CephException(Integer code, String msg) {
        this.code = code;
        this.msg = msg;
    }

    public Integer getCode() {
        return code;
    }

    public void setCode(Integer code) {
        this.code = code;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }
}
