package com.mtty;

import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Created by zhuqiwen on 2017/2/17.
 */
public class LabeledVector {
    private int label;
    private SortedMap<Integer, Integer> features = new TreeMap<>();

    public int getLabel() {
        return label;
    }

    public void setLabel(int label) {
        this.label = label;
    }

    public SortedMap<Integer, Integer> getFeatures() {
        return features;
    }

    public void setFeatures(SortedMap<Integer, Integer> features) {
        this.features = features;
    }

    @Override
    public String toString() {
        return label + " " +
                features.entrySet().stream()
                        .map(e -> e.getKey() + ":" + e.getValue())
                        .reduce((s1, s2) -> s1 + " " + s2).get();
    }
}
