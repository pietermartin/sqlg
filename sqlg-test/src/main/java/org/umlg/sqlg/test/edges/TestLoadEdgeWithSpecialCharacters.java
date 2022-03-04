package org.umlg.sqlg.test.edges;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class TestLoadEdgeWithSpecialCharacters extends BaseTest {

    @Before
    public void beforeTest() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsStreamingBatchMode());
    }

    @Test
    public void testBulkAddEdgesWithSpecialCharactersInValues() {
        this.sqlgGraph.tx().normalBatchModeOn();
        List<Pair<String, String>> uids = new ArrayList<>();
        uids.add(Pair.of("Telefonica->UK->MAVENIR->IMC->REAL_WS->ELL_CMS-ELL-45->APP9->ims->IFC_xml->VOLTE-MSIM->dummyTagDoNotUseForWriteback->InitialFilterCriteria->341->TriggerPoint->1->SPT","Telefonica->UK->MAVENIR->IMC->REAL_WS->ELL_CMS-ELL-45->APP9->ims->IFC_xml->VOLTE-MSIM->dummyTagDoNotUseForWriteback->InitialFilterCriteria->341->TriggerPoint->1->SPT->SIPHeader->Accept-Contact->\\+g\\.3gpp\\.icsi-ref=\"urn%3Aurn-7%3A3gpp-service\\.ims\\.icsi\\.mmtel\""));
        uids.add(Pair.of("Telefonica->UK->MAVENIR->IMC->REAL_WS->ELL_CMS-ELL-45->APP9->ims->IFC_xml->VOLTE-MSIM->dummyTagDoNotUseForWriteback->InitialFilterCriteria->341->TriggerPoint->1->SPT","Telefonica->UK->MAVENIR->IMC->REAL_WS->ELL_CMS-ELL-45->APP9->ims->IFC_xml->VOLTE-MSIM->dummyTagDoNotUseForWriteback->InitialFilterCriteria->341->TriggerPoint->1->SPT->SIPHeader->P-Asserted-Service->3gpp-service\\.ims\\.icsi\\.mmtel"));
        uids.add(Pair.of("Telefonica->UK->MAVENIR->IMC->REAL_WS->ELL_CMS-ELL-45->APP5->ims->IFC_xml->VOLTE-MSIM->dummyTagDoNotUseForWriteback->InitialFilterCriteria->400->TriggerPoint->1->SPT","Telefonica->UK->MAVENIR->IMC->REAL_WS->ELL_CMS-ELL-45->APP5->ims->IFC_xml->VOLTE-MSIM->dummyTagDoNotUseForWriteback->InitialFilterCriteria->400->TriggerPoint->1->SPT->SIPHeader->Contact->\\+g\\.3gpp\\.smsip"));
        uids.add(Pair.of("Telefonica->UK->MAVENIR->IMC->REAL_WS->HLW_CMS-BAS-42->APP17->ims->IFC_xml->VOLTE->dummyTagDoNotUseForWriteback->InitialFilterCriteria->720->TriggerPoint->1->SPT","Telefonica->UK->MAVENIR->IMC->REAL_WS->HLW_CMS-BAS-42->APP17->ims->IFC_xml->VOLTE->dummyTagDoNotUseForWriteback->InitialFilterCriteria->720->TriggerPoint->1->SPT->SIPHeader->Accept-Contact->\\+g\\.3gpp\\.icsi-ref=\"urn%3Aurn-7%3A3gpp-service\\.ims\\.icsi\\.mmtel\""));

        Set<String> parent = uids.stream().map(Pair::getLeft).collect(Collectors.toSet());
        parent.forEach(s -> this.sqlgGraph.addVertex(T.label, "SPT", "cm_uid", s));
        uids.forEach(p -> this.sqlgGraph.addVertex(T.label, "SIPHeader", "cm_uid", p.getRight()));

        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().streamingBatchModeOn();
        this.sqlgGraph.bulkAddEdges(
                "SPT",
                "SIPHeader",
                "SPT_SIPHeader",
                Pair.of("cm_uid", "cm_uid"),
                uids);
        this.sqlgGraph.tx().commit();

        assertEquals(3, this.sqlgGraph.traversal().V().hasLabel("SPT").count().next(), 0);
        assertEquals(4, this.sqlgGraph.traversal().V().hasLabel("SIPHeader").count().next(), 0);
        assertEquals(4, this.sqlgGraph.traversal().V().hasLabel("SPT").out().count().next(), 0);
    }

}
