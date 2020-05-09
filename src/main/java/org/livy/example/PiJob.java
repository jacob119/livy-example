package org.livy.example;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.livy.*;

import java.util.ArrayList;
import java.util.List;

public class PiJob implements Job<Double>,
        Function<Integer, Integer>,
        Function2<Integer, Integer, Integer> {

    private final int slices;
    private final int samples;

    public PiJob(int slices) {
        this.slices = slices;
        this.samples = (int) Math.min(100000L * slices, Integer.MAX_VALUE);
    }


    public Double call(JobContext ctx) throws Exception {
        List<Integer> sampleList = new ArrayList<>();
        for (int i = 0; i < samples; i++) {
            sampleList.add(i);
        }

        return 4.0d * ctx.sc().parallelize(sampleList, slices).map(this).reduce(this) / samples;
    }

    @Override
    public Integer call(Integer v1) {
        double x = Math.random() * 2 - 1;
        double y = Math.random() * 2 - 1;
        return (x * x + y * y < 1) ? 1 : 0;
    }

    @Override
    public Integer call(Integer v1, Integer v2) {
        return v1 + v2;
    }
}
