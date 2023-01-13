package uk.gov.legislation.research.ngrams.api;

import uk.gov.legislation.research.qb.Attribute;
import uk.gov.legislation.research.qb.DataCube;
import uk.gov.legislation.research.qb.Dimension;
import uk.gov.legislation.research.qb.Measure;
import uk.gov.legislation.research.Legislation;
import uk.gov.legislation.research.ngrams.Counts;
import uk.gov.legislation.research.ngrams.Ngrams;
import org.apache.jena.vocabulary.XSD;
import org.json.JSONException;
import org.json.JSONWriter;

import java.io.*;
import java.util.*;
import java.util.Map.Entry;

public class CountsFormatter implements Counts {

	private static NavigableMap<Short, LinkedHashMap<String, Integer>> prunePadAndInvert(LinkedHashMap<String, NavigableMap<Short, Integer>> counts, DateRange years) {
		NavigableMap<Short, LinkedHashMap<String, Integer>> inverted = new TreeMap<>();
		for (short year = (short) years.start; year <= years.end; year++) {
			LinkedHashMap<String, Integer> x = new LinkedHashMap<>();
			for (String ngram : counts.keySet()) {
				Integer count = counts.get(ngram).getOrDefault(year, 0);
				x.put(ngram, count);
			}
			inverted.put(year, x);
		}
		return inverted;
	}

	private Legislation.Searchable legType;
	private DateRange years;
	private LinkedHashSet<String> ngrams;
	private SortedSet<Integer> n;
	private Ngrams.Type ngType;
	private SortedMap<Short, LinkedHashMap<String, Integer>> counts;
	private ArrayList<Map<Short, Double>> scales;

	CountsFormatter(Legislation.Searchable legType, Ngrams.Type ngType, LinkedHashSet<String> ngrams, LinkedHashMap<String, NavigableMap<Short, Integer>> counts, DateRange years) throws IOException {
		this.legType = legType;
		this.ngType = ngType;
		this.counts = prunePadAndInvert(counts, years);
		this.ngrams = ngrams;
		this.years = years;
		n = new TreeSet<>();
		for (String ngram : ngrams)
			n.add(ngram.split(" ").length);
		scales = ScalesCache.get(legType, ngType);
	}

	public void json(Writer writer) {
		JSONWriter json = new JSONWriter(writer);
		try {
			json.array();
			for (Entry<Short, LinkedHashMap<String, Integer>> entry : counts.entrySet()) {
				json.object();
				Short year = entry.getKey();
				json.key("year").value(year);
				json.key("counts");
				json.object();
				for (Entry<String, Integer> entry2 : entry.getValue().entrySet()) {
					String ngram = entry2.getKey();
					json.key(ngram);
					Integer count = entry2.getValue();
					json.value(count);
				}
				json.endObject();
				json.key("scales");
				json.object();
				for (Integer n : this.n)
					json.key(n.toString()).value(scales.get(n).getOrDefault(year, 0.0));
				json.endObject();
				json.endObject();
			}
			json.endArray();
		} catch (JSONException e) {
			throw new RuntimeException("error writing JSON", e);
		}
	}
	
	public String json() {
		StringWriter writer = new StringWriter();
		json(writer);
		return writer.toString();
	}
	
	public void csv(PrintWriter writer) {
		CSV csv = new CSV(writer);
		int rowLength = 1 + scales.size() + ngrams.size();
		List<String> headings = new ArrayList<>(rowLength);
		headings.add("_year");
		headings.addAll(ngrams);
		for (Integer n : this.n) headings.add("_scale_" + n);
		csv.writeln(headings);
		for (Entry<Short, LinkedHashMap<String, Integer>> entry : counts.entrySet()) {
			List<Number> values = new ArrayList<>(rowLength);
			Short year = entry.getKey();
			values.add(year);
			for (Integer count : entry.getValue().values()) values.add(count);
			for (Integer n : this.n) values.add(scales.get(n).get(year));
			csv.writeln(values);
		}
	}
	
	public void tsv(PrintWriter writer) {
		int rowLength = 1 + scales.size() + ngrams.size();
		List<String> headings = new ArrayList<>(rowLength);
		headings.add("_year");
		headings.addAll(ngrams);
		for (Integer n : this.n) headings.add("_scale_" + n);
		TSV.line(writer, headings);
		for (Entry<Short, LinkedHashMap<String, Integer>> entry : counts.entrySet()) {
			List<Number> values = new ArrayList<>(rowLength);
			Short year = entry.getKey();
			values.add(year);
			for (Integer count : entry.getValue().values()) values.add(count);
			for (Integer n : this.n) values.add(scales.get(n).get(year));
			TSV.line(writer, values);
		}
	}

	public static void csvMetadata(Legislation.Searchable legType, SortedSet<Integer> years, LinkedHashSet<String> ngrams, Ngrams.Type ngType, Writer writer, String url) {
		Map<String, Object> output = new LinkedHashMap<>();
		output.put("@context", "http://www.w3.org/ns/csvw");
		output.put("url", url);
		Map<String, Object> notes = new LinkedHashMap<>();
		notes.put("legislationType", legType.name());
		notes.put("dateRange", Arrays.asList(years.first(), years.last()));
		notes.put("ngrams", ngrams);
		notes.put("ngramType", ngType.description);
		output.put("notes", notes);
		Map<String, Object> tableSchema = new LinkedHashMap<>();
		List<Map<String, Object>> columns = new ArrayList<>();
		Map<String, Object> column1 = new LinkedHashMap<>();
		column1.put("titles", "_year");
		Map<String, Object> datatype = new LinkedHashMap<>();
		datatype.put("base", "integer");
		datatype.put("minimum", years.first());
		datatype.put("maximum", years.last());
		column1.put("datatype", datatype);
		column1.put("dc:description", "the year");
		columns.add(column1);
		for (String ngram : ngrams) {
			Map<String, Object> column = new LinkedHashMap<>();
			column.put("titles", ngram);
			column.put("datatype", "integer");
			column.put("dc:description", "the number of occurrances of the term '" + ngram + "' in the given year");
			columns.add(column);
		}
		// add scale columns
		SortedSet<Integer> ns = new TreeSet<>();
		for (String ngram : ngrams) ns.add(ngram.split(" ").length);
		for (Integer n : ns) {
			Map<String, Object> column = new LinkedHashMap<>();
			column.put("titles", "_scale_" + n);
			column.put("datatype", "double");
			column.put("dc:description", "the scaling factor for " + n + "-grams in the given year");
			columns.add(column);
		}
		tableSchema.put("columns", columns);
		tableSchema.put("primaryKey", "_year");
		output.put("tableSchema", tableSchema);
		Map<String, Object> dialect = new LinkedHashMap<>();
		dialect.put("delimiter", ",");
		dialect.put("header", true);
		output.put("dialect", dialect);
		new JSON(writer).write(output);
	}
	
	private String makeDescription() {
		ArrayList<String> list = new ArrayList<>(ngrams);
		String terms;
		if (list.size() == 1) {
			terms = "term '" + list.get(0) + "'";
		} else {
			terms = "terms '" + String.join("', '", list.subList(0, list.size() - 1)) + "' and '" + list.get(list.size() - 1) + "'";
		}
		StringBuilder title = new StringBuilder();
		title.append("The yearly counts of the occurrences of the ");
		title.append(terms);
		title.append(" from ");
		title.append(years.start);
		title.append(" to ");
		title.append(years.end);
		title.append(".");
		return title.toString();
	}
	
	private DataCube toDataCube() {
		DataCube qb = new DataCube("exp", "http://research.legislation.gov.uk/namespaces/explorer#");
		qb.addTitle("Yearly counts");
		qb.addDescription(makeDescription());
		Dimension typeDimension = qb.addDimention("type", XSD.xstring).setLabel("the legislative type");
		Dimension ngramDimension = qb.addDimention("ngram", XSD.xstring).setLabel("the word or phrase counted");
		Dimension yearDimension = qb.addDimention("year", XSD.gYear).setLabel("the year");
		Attribute scaleAttribute = qb.addAttribute("scale").setLabel("the scaling factor");
		Measure countMeasure = qb.addMeasure("count").setLabel("the number of occurrences of the word or phrase");
		for (Entry<Short, LinkedHashMap<String, Integer>> entry1 : counts.entrySet()) {
			Short year = entry1.getKey();
			for (Entry<String, Integer> entry2 : entry1.getValue().entrySet()) {
				String ngram = entry2.getKey();
				int n = ngram.split(" ").length;
				Integer count = entry2.getValue();
				Double scale = scales.get(n).get(year);
				qb.addObservation()
					.setDimension(typeDimension, legType.name())
					.setDimension(ngramDimension, ngram)
					.setDimension(yearDimension, year)
					.setMeasure(countMeasure, count)
					.setAttribute(scaleAttribute, scale);
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
