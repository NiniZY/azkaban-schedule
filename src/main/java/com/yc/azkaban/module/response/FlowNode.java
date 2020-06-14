package com.yc.azkaban.module.response;

import java.util.Arrays;

public class FlowNode {
    String id;
    String type;
    String[] in;
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String[] getIn() {
        return in;
    }

    public void setIn(String[] in) {
        this.in = in;
    }

    @Override
    public String toString() {
        return "FlowNode{" +
                "id='" + id + '\'' +
                ", type='" + type + '\'' +
                ", in=" + Arrays.toString(in) +
                '}';
    }
}
