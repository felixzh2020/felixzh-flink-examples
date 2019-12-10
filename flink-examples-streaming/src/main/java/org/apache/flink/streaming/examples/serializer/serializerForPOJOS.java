package org.apache.flink.streaming.examples.serializer;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class serializerForPOJOS {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Person> dataStream = env.fromElements(new Person("a", 1), new Person("b", 2), new Person("c", 3));

        DataStream dataStream1 = dataStream.filter(new FilterFunction<Person>() {
            @Override
            public boolean filter(Person person) {
                return person.getAge() >= 2;
            }
        });

        dataStream1.print();

        env.execute();
    }

}

class Person {
    private String name;
    private Integer age;

    public Person() {
    }

    public Person(String name, Integer age) {
        this.name = name;
        this.age = age;
    }

    public String getName() {
        return name;
    }

    public Integer getAge() {
        return age;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    @Override
    public String toString() {
        return "Person{" +
                "name='" + name + '\'' +
                ", age=" + age +
                '}';
    }
}
