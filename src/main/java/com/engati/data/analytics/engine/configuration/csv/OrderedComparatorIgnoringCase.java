package com.engati.data.analytics.engine.configuration.csv;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class OrderedComparatorIgnoringCase implements Comparator<String> {

    private List<String> predefinedOrder;

    public OrderedComparatorIgnoringCase(String[] predefinedOrder) {
        this.predefinedOrder = new ArrayList<>();
        for (String item : predefinedOrder) {
            this.predefinedOrder.add(item.toLowerCase());
        }
    }

    @Override
    public int compare(String o1, String o2) {
        return predefinedOrder.indexOf(o1.toLowerCase()) - predefinedOrder.indexOf(o2.toLowerCase());
    }
}

