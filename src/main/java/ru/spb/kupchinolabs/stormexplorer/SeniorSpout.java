package ru.spb.kupchinolabs.stormexplorer;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;

public class SeniorSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private static int counter = 0;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("from", "greeting", "counter"));
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        Utils.sleep(2000);
        counter++;
        collector.emit(new Values("Senior", "Happy birthday, jun, #" + counter, counter), counter);
    }

    @Override
    public void ack(Object msgId) {
        System.out.println("SeniorSpout ack() " + msgId);
    }

    @Override
    public void fail(Object msgId) {
        System.out.println("SeniorSpout fail() " + msgId);
        collector.emit(new Values("Senior", "Happy birthday, jun, #" + msgId, msgId), msgId);
    }
}
