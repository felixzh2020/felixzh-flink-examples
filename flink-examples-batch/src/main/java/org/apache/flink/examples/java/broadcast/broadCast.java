package org.apache.flink.examples.java.broadcast;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;

import java.util.Collection;

/**
 * @ClassName broadCast
 * @Description 广播变量案例
 * @Author felixzh
 * @Date 2018/12/24 13:50
 **/
public class broadCast {
    public static void main(String []args) throws Exception{
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 1. 定义需要广播的广播变量DataSet
        DataSet<Integer> toBroadcast = env.fromElements(1, 2, 3);

        DataSet<String> data = env.fromElements("a", "b");

        DataSet<String> data1 = data.map(new RichMapFunction<String, String>() {
            Collection<Integer> broadcastSet = null;
            @Override
            public void open(Configuration parameters) throws Exception {
                // 3. 获取广播变量DataSet
                broadcastSet = getRuntimeContext().getBroadcastVariable("broadcastSetName");
            }

            @Override
            public String map(String value) throws Exception {
                return value + ":" + broadcastSet.toString();
            }
        }).withBroadcastSet(toBroadcast, "broadcastSetName"); // 2. 执行广播DataSet动作

        data.print();
        System.out.println("------------------------");
        data1.print();
    }
}
