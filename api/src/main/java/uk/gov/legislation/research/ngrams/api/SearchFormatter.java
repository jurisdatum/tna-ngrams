package uk.gov.legislation.research.ngrams.api;

import uk.gov.legislation.research.qb.Attribute;
import uk.gov.legislation.research.qb.DataCube;
import uk.gov.legislation.research.qb.Dimension;
import uk.gov.legislation.research.qb.Measure;
import uk.gov.legislation.research.ngrams.Ngrams;
import org.apache.jena.vocabulary.XSD;

import java.util.*;
import java.util.stream.Collectors;

public class SearchFormatter {

    static SortedMap<Integer, LinkedHashMap<String, Long>> untokenize(SortedMap<Integer, LinkedHashMap<String, Long>> results) {
        return results.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, (e1) -> {
                return e1.getValue().entrySet().stream().collect(Collectors.toMap((e2) -> Ngrams.untokenize(e2.getKey()), Map.Entry::getValue, (x, y) -> y, LinkedHashMap::new));
            }, (a, b) -> b, TreeMap::new));
    }
    static DataCube toDataCube(LinkedHashSet<String> components, SortedMap<Integer, LinkedHashMap<String, Long>> results) {
        DataCube qb = new DataCube("exp", "http://research.legislation.gov.uk/namespaces/explorer#");
        qb.addTitle("Search results");
        qb.addDescription("n-grams containing: " + String.join(", ", components));
        Attribute ngAttr = qb.addAttribute("ngram");
        Dimension dim = qb.addDimention("n", XSD.integer);
        Measure measure = qb.addMeasure("count");
        for (Map.Entry<Integer, LinkedHashMap<String, Long>> result : results.entrySet()) {
            Integer n = result.getKey();
            for (Map.Entry<String, Long> count : result.getValue().entrySet()) {
                String ngram = count.getKey();
                Long value = count.getValue();
                qb.addObservation()
                        .setDimension(dim, n)
                        .setAttribute(ngAttr, ngram)
                        .setMeasure(measure, value);
            }
        }
        return qb;
    };

}
