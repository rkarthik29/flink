package com.cloudera.example.flink.udf;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple9;

public class MyJoinFunction implements JoinFunction<String, String, Tuple9<String, String, String, String, String, String, String, String, Integer>> {
    /**
     * 
     */
    private static final long serialVersionUID = 7309257335950509291L;

    @Override
    public Tuple9<String, String, String, String, String, String, String, String, Integer> join(String first, String second) throws Exception {
        // TODO Auto-generated method stub

        String speed = second.split("\\|")[8];
        String[] geoFields = first.split("\\|");
        StringBuilder sb = new StringBuilder();
        sb.append(geoFields[0]);
        sb.append(",");
        sb.append(geoFields[1]);
        sb.append(",");
        sb.append(geoFields[3]);
        sb.append("");
        sb.append(geoFields[4]);
        sb.append(",");
        sb.append(geoFields[5]);
        sb.append(",");
        sb.append(geoFields[6]);
        sb.append(",");
        sb.append(geoFields[7]);
        sb.append(",");
        sb.append(geoFields[8]);
        sb.append(",");
        sb.append(geoFields[9]);
        sb.append(",");
        sb.append(speed);
        Tuple9<String, String, String, String, String, String, String, String, Integer> tuple = new Tuple9<String, String, String, String, String, String, String, String, Integer>();
        tuple.setFields(geoFields[0], geoFields[1], geoFields[3] + geoFields[4], geoFields[5], geoFields[6], geoFields[7], geoFields[8], geoFields[9], Integer.parseInt(speed));
        return tuple;
    }
}
