package com.hadoop.mr.v2;

import org.apache.hadoop.mapreduce.v2.MiniMRYarnCluster;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;

public class ClusterBuilder {

    protected static MiniMRYarnCluster miniCluster;

    public static void setUpCluster() throws Exception {
        YarnConfiguration conf = new YarnConfiguration();
        conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 64);
        conf.setClass(YarnConfiguration.RM_SCHEDULER, FifoScheduler.class, ResourceScheduler.class);
        miniCluster = new MiniMRYarnCluster("WordCountTest");
        miniCluster.init(conf);
        miniCluster.start();
    }

    public static void tearDownCluster() throws Exception {
         miniCluster.close();
    }
}
