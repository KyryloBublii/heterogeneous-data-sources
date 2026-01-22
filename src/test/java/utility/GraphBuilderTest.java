package utility;

import org.example.models.entity.Relationship;
import org.example.service.transform.GraphBuilder;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class GraphBuilderTest {

    @Test
    void build_EmptyRelationships_ReturnsEmptyGraph() {
        List<Relationship> relationships = Collections.emptyList();

        GraphBuilder.Graph graph = GraphBuilder.build(relationships);

        assertNotNull(graph);
        assertTrue(graph.nodes().isEmpty());
    }

    @Test
    void build_SingleRelationship_CreatesTwoNodes() {
        org.example.models.entity.Relationship rel = new org.example.models.entity.Relationship();
        rel.setFromType("customer");
        rel.setFromId("CUST-1");
        rel.setToType("order");
        rel.setToId("ORD-1");
        rel.setRelationType("has_order");

        GraphBuilder.Graph graph = GraphBuilder.build(Arrays.asList(rel));

        assertNotNull(graph);
        assertEquals(2, graph.nodes().size());
    }

    @Test
    void build_MultipleRelationships_BuildsConnectedGraph() {
        org.example.models.entity.Relationship rel1 = new org.example.models.entity.Relationship();
        rel1.setFromType("customer");
        rel1.setFromId("CUST-1");
        rel1.setToType("order");
        rel1.setToId("ORD-1");
        rel1.setRelationType("has_order");

        org.example.models.entity.Relationship rel2 = new org.example.models.entity.Relationship();
        rel2.setFromType("order");
        rel2.setFromId("ORD-1");
        rel2.setToType("product");
        rel2.setToId("PROD-1");
        rel2.setRelationType("contains_product");

        GraphBuilder.Graph graph = GraphBuilder.build(Arrays.asList(rel1, rel2));

        assertNotNull(graph);
        assertTrue(graph.nodes().size() >= 2);
    }

    @Test
    void nodeRef_EqualityAndHashCode_WorksCorrectly() {
        GraphBuilder.NodeRef node1 = new GraphBuilder.NodeRef("customer", "CUST-1");
        GraphBuilder.NodeRef node2 = new GraphBuilder.NodeRef("customer", "CUST-1");
        GraphBuilder.NodeRef node3 = new GraphBuilder.NodeRef("customer", "CUST-2");

        assertEquals(node1, node2);
        assertNotEquals(node1, node3);
        assertEquals(node1.hashCode(), node2.hashCode());
        assertNotEquals(node1.hashCode(), node3.hashCode());
    }

    @Test
    void neighbors_ValidNode_ReturnsEdges() {
        org.example.models.entity.Relationship rel = new org.example.models.entity.Relationship();
        rel.setFromType("customer");
        rel.setFromId("CUST-1");
        rel.setToType("order");
        rel.setToId("ORD-1");
        rel.setRelationType("has_order");

        GraphBuilder.Graph graph = GraphBuilder.build(Arrays.asList(rel));
        GraphBuilder.NodeRef customerNode = new GraphBuilder.NodeRef("customer", "CUST-1");

        List<GraphBuilder.Edge> neighbors = graph.neighbors(customerNode);

        assertNotNull(neighbors);
    }
}