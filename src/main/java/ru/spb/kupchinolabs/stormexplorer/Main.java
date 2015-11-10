package ru.spb.kupchinolabs.stormexplorer;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class Main {
    public static void main(String[] args) {
        Config conf = new Config();
        conf.registerMetricsConsumer(backtype.storm.metric.LoggingMetricsConsumer.class);
        conf.setDebug(true);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("senior", new SeniorSpout());
        builder.setBolt("junior", new JuniorBolt()).shuffleGrouping("senior");

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("greetings", conf, builder.createTopology());

        Utils.sleep(120000);

        cluster.killTopology("greetings");
        cluster.shutdown();
    }
}
