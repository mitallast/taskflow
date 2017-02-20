package org.github.mitallast.taskflow.common;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.typesafe.config.ConfigFactory;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import org.github.mitallast.taskflow.common.json.JsonService;
import org.github.mitallast.taskflow.dag.Dag;
import org.github.mitallast.taskflow.dag.Task;
import org.github.mitallast.taskflow.operation.OperationCommand;
import org.github.mitallast.taskflow.operation.OperationEnvironment;
import org.junit.Assert;
import org.junit.Test;

public class JsonSpec {

    private final JsonService jsonService = new JsonService(ConfigFactory.defaultReference());

    @Test
    public void testSerializeDag() throws Exception {
        Dag dag = new Dag(
            123,
            1234,
            "test_dag",
            ImmutableList.of(
                new Task(
                    12455,
                    345,
                    "test_task",
                    ImmutableSet.of("test_task"),
                    3,
                    "dummy",
                    new OperationCommand(
                        ConfigFactory.parseString("test=test"),
                        new OperationEnvironment(
                            ImmutableMap.of("test", "test")
                        )
                    )
                )
            )
        );

        ByteBuf buffer = Unpooled.buffer();
        System.out.println(dag);
        jsonService.serialize(buffer, dag);
        System.out.println(buffer.toString(CharsetUtil.UTF_8));

        Dag dag2 = jsonService.deserialize(buffer, Dag.class);
        Assert.assertEquals(dag2, dag);
    }
}
