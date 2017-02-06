package org.github.mitallast.taskflow.common;

import org.jgrapht.experimental.dag.DirectedAcyclicGraph;
import org.jgrapht.graph.DefaultEdge;
import org.junit.Test;

public class JGraphTest {

    @Test
    public void test() throws Exception {
        DirectedAcyclicGraph<String, DefaultEdge> dag = new DirectedAcyclicGraph<>(DefaultEdge.class);

        dag.addVertex("not connected");
        dag.addVertex("1");
        dag.addVertex("2");
        dag.addVertex("3");
        dag.addVertex("4");
        dag.addVertex("5");

        dag.addDagEdge("1", "2");
        dag.addDagEdge("1", "3");
        dag.addDagEdge("2", "3");
        dag.addDagEdge("2", "3");
        dag.addDagEdge("3", "4");
        dag.addDagEdge("1", "5");
        dag.addDagEdge("4", "5");

        for (String s : dag) {
            System.out.println(s);
        }
    }
}
