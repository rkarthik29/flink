package com.cloudera.example.flink.udf;

import org.apache.flink.api.java.functions.KeySelector;

public class MyKeySelector implements KeySelector<String, Integer> {
    /**
    *
    */
    private static final long serialVersionUID = 1L;

    @Override
    public Integer getKey(String value) throws Exception {
        String[] fields = value.split("\\|");
        return Integer.parseInt(fields[3] + fields[4]);
    }
}