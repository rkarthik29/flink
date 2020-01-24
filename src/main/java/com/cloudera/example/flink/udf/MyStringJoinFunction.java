package com.cloudera.example.flink.udf;

import org.apache.flink.api.common.functions.JoinFunction;

public class MyStringJoinFunction implements JoinFunction<String, String, String> {
    /**
     * 
     */
    private static final long serialVersionUID = 7309257335950509291L;

    @Override
    public String join(String first, String second) throws Exception {
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

        return sb.toString();
    }
}
