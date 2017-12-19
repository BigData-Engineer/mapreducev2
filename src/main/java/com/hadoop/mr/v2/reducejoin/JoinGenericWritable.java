package com.hadoop.mr.v2.reducejoin;

import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.Writable;

@SuppressWarnings("unchecked")
public class JoinGenericWritable extends GenericWritable {

    private static Class<? extends Writable>[] CLASSES = null;

    static {
        CLASSES = (Class<? extends Writable>[]) new Class[] { SalesOrderDataRecord.class, ProductRecord.class, com.hadoop.mr.v2.mapsidejoin.ProductRecord.class };
    }

    public JoinGenericWritable() {
    }

    public JoinGenericWritable(Writable instance) {
        set(instance);
    }

    @Override
    protected Class<? extends Writable>[] getTypes() {
        return CLASSES;
    }

}
