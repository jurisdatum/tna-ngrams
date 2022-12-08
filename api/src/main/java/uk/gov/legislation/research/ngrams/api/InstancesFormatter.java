package uk.gov.legislation.research.ngrams.api;

import uk.gov.legislation.research.qb.Attribute;
import uk.gov.legislation.research.qb.DataCube;
import uk.gov.legislation.research.qb.Measure;
import uk.gov.legislation.research.qb.Observation;
import uk.gov.legislation.research.Legislation;
import uk.gov.legislation.research.ngrams.DocumentCounts;
import uk.gov.legislation.research.ngrams.Ngrams;
import org.json.JSONException;
import org.json.JSONWriter;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.*;

public class InstancesFormatter {

    private static Map<String, Map<String, Integer>> prune(LinkedHashMap<String, NavigableMap<Short, LinkedHashMap<String, Integer>>> counts, DateRange years) {
        Map<String, Map<String, Integer>> pruned = new LinkedHashMap<>();
        for (Map.Entry<String, NavigableMap<Short, LinkedHashMap<String, Integer>>> entry1 : counts.entrySet()) {
            Map<String, Integer> pruned1 = new HashMap<>();
            for (short year = (short) years.start; year <= years.end; year++)
                if (entry1.getValue().containsKey(year))
                    pruned1.putAll(entry1.getValue().get(year));
            pruned.put(entry1.getKey(), pruned1);
        }
        return pruned;
    }
    private static LinkedHashMap<String, Map<String, Integer>> pruneInvertAndSort(LinkedHashMap<String, NavigableMap<Short, LinkedHashMap<String, Integer>>> counts, DateRange years) {
        Map<String, Map<String, Integer>> pruned = prune(counts, years);
        Map<String, Map<String, Integer>> inverted = DocumentCounts.invert(pruned);
        return DocumentCounts.sort(inverted);
    }

    final LinkedHashMap<String, Map<String, Integer>> counts;	// docIds to map of ngrams to counts
    final Map<String, String> titles;	// docIds to titles

    InstancesFormatter(LinkedHashMap<String, NavigableMap<Short, LinkedHashMap<String, Integer>>> counts, DateRange dates) throws IOException {
        this.counts = pruneInvertAndSort(counts, dates);
        this.titles = uk.gov.legislation.research.ngrams.hadoop.hbase.Documents.getTitles(HBase.config(), this.counts.keySet());
    }

    private static final String prefix = "http://www.legislation.gov.uk/id/";

    public void json(PrintWriter writer) {
        try {
            JSONWriter json = new JSONWriter(writer);
            json.array();
            for (Map.Entry<String, Map<String, Integer>> entry : counts.entrySet()) {
                json.object();
                String docId = entry.getKey();
                json.key("id").value(prefix + docId);
                String title = titles.get(docId);
                json.key("title").value(title);
                json.key("counts");
                json.object();
                for (Map.Entry<String, Integer> entry2 : entry.getValue().entrySet()) {
                    String ngram = entry2.getKey();
                    Integer count = entry2.getValue();
                    json.key(ngram).value(count);
                }
                json.endObject();
                json.endObject();
            }
            json.endArray();
        } catch (JSONException e) {
            throw new RuntimeException("error writing JSON", e);
        }
    }

    public void csv(PrintWriter writer) {
        CSV csv = new CSV(writer);
        csv.writeln("_id", "_title", counts.values().iterator().next().keySet());
        for (Map.Entry<String, Map<String, Integer>> e1 : counts.entrySet()) {
            String id = e1.getKey();
            String title = titles.get(id);
            Collection<Integer> c = e1.getValue().values();
            csv.writeln(prefix + id, title, c);
        }
    }

    public void tsv(PrintWriter printer) {
        TSV.line(printer, Arrays.asList("_id", "_title", counts.values().iterator().next().keySet()));
        for (Map.Entry<String, Map<String, Integer>> e1 : counts.entrySet()) {
            String id = e1.getKey();
            String title = titles.get(id);
            Collection<Integer> c = e1.getValue().values();
            TSV.line(printer, Arrays.asList(prefix + id, title, c));
        }
    }

    private static void csvMetadata(Legislation.Searchable legType, String[] dateRange, LinkedHashSet<String> ngrams, Ngrams.Type ngType, PrintWriter writer, String url) {
        Map<String, Object> output = new LinkedHashMap<>();
        output.put("@context", "http://www.w3.org/ns/csvw");
        output.put("url", url);
        Map<String, Object> notes = new LinkedHashMap<>();
        notes.put("legislationType", legType.name());
        notes.put("dateRange", dateRange);
        notes.put("ngramType", ngType.toString());
        output.put("notes", notes);
        Map<String, Object> tableSchema = new LinkedHashMap<>();
        List<Map<String, Object>> columns = new ArrayList<>();
        Map<String, Object> column1 = new LinkedHashMap<>();
        column1.put("titles", "_id");
        column1.put("datatype", "anyURI");
        column1.put("dc:description", "the URI of the document containing the given terms");
        columns.add(column1);
        Map<String, Object> column2 = new LinkedHashMap<>();
        column2.put("titles", "_title");
        column2.put("datatype", "string");
        column2.put("dc:description", "the title of the document containing the given terms");
        columns.add(column2);
        for (String ngram : ngrams) {
            Map<String, Object> column = new LinkedHashMap<>();
            column.put("titles", ngram);
            column.put("datatype", "integer");
            column.put("dc:description", "the number of occurrances of the term '" + ngram + "' in the given document");
            columns.add(column);
        }
        tableSchema.put("columns", columns);
        tableSchema.put("primaryKey", "_document");
        output.put("tableSchema", tableSchema);
        Map<String, Object> dialect = new LinkedHashMap<>();
        dialect.put("delimiter", ",");
        dialect.put("header", true);
        output.put("dialect", dialect);
        new JSON(writer).write(output);
    }

    public static void csvMetadata(Legislation.Searchable legType, SortedSet<Integer> years, LinkedHashSet<String> ngrams, Ngrams.Type ngType, PrintWriter writer, String url) {
        String[] dateRange = new String[] { years.first().toString(), years.last().toString() };
        csvMetadata(legType, dateRange, ngrams, ngType, writer, url);
    }

    public static void csvMetadata(Legislation.Searchable legType, SortedMap<Integer, SortedSet<Integer>> months, LinkedHashSet<String> ngrams, Ngrams.Type ngType, PrintWriter writer, String url) {
        String[] dateRange = new String[] {
                months.firstKey() + "-" + String.format("%02d", months.get(months.firstKey()).first()),
                months.lastKey() + "-" + String.format("%02d", months.get(months.lastKey()).last())
        };
        csvMetadata(legType, dateRange, ngrams, ngType, writer, url);
    }

    private String makeDescription(Set<String> ngrams) {
        ArrayList<String> list = new ArrayList<>(ngrams);
        String terms;
        if (list.size() == 1) {
            terms = "term '" + list.get(0) + "'";
        } else {
            terms = "terms '" + String.join("', '", list.subList(0, list.size() - 1)) + "' and '" + list.get(list.size() - 1) + "'";
        }
        StringBuilder title = new StringBuilder();
        title.append("The counts in each document of the occurrences of the ");
        title.append(terms);
        title.append(".");
        return title.toString();
    }
    private DataCube toDataCube() {
        Set<String> ngrams = counts.isEmpty() ? Collections.emptySet() : counts.values().iterator().next().keySet();
        DataCube qb = new DataCube("exp", "http://research.legislation.gov.uk/namespaces/explorer#");
        qb.addTitle("Instance counts");
        qb.addDescription(makeDescription(ngrams));
        Attribute idAttr = qb.addAttribute("id").setLabel("the id of the document");
        Attribute titleAttr = qb.addAttribute("title").setLabel("the title of the document");
        Map<String, Measure> measures = new LinkedHashMap<>();
        for (String ngram : ngrams) {
            Measure measure = qb.addMeasure(ngram).setLabel("the number of occurrences of the word or phrase: " + ngram);
            measures.put(ngram, measure);
        }
        for (Map.Entry<String, Map<String, Integer>> entry1 : counts.entrySet()) {
            String docId = entry1.getKey();
            String docTitle = titles.get(docId);
            Observation ob = qb.addObservation()
                    .setAttribute(idAttr, docId)
                    .setAttribute(titleAttr, docTitle);
            for (Map.Entry<String, Integer> docCounts : entry1.getValue().entrySet()) {
                String ngram = docCounts.getKey();
                Integer count = docCounts.getValue();
                Measure measure = measures.get(ngram);
                ob.setMeasure(measure, count);
            }
        }
        return qb;
    }

    public void rdf(OutputStream output) {
        DataCube qb = toDataCube();
        qb.write(output, DataCube.Format.XML);
    }

    public void turtle(OutputStream output) {
        DataCube qb = toDataCube();
        qb.write(output, DataCube.Format.Turtle);
    }

}
