package com.hadoop.mr.v2;

import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class WordCountMapperTest extends ClusterBuilder {
    
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        setUpCluster();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        tearDownCluster();
    }

    @Before
    public void setUp() throws Exception {
        
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testMapLongWritableTextContext() {
        fail("Not yet implemented");
    }

}
