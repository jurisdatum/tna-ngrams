package uk.gov.legislation.research.ngrams.api;

import static uk.gov.legislation.research.ngrams.api.Explorer.getLegType;
import static uk.gov.legislation.research.ngrams.api.Explorer.getNgrams;
import static uk.gov.legislation.research.ngrams.api.Explorer.getNgramsType;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.regex.Pattern;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import uk.gov.legislation.research.qb.DataCube;
import uk.gov.legislation.research.Legislation;
import uk.gov.legislation.research.ngrams.Ngrams;

@WebServlet("/search/*")
public class Search  extends HttpServlet {
	
	static final Pattern pattern = Pattern.compile("^/search/[a-z]{3,9}(/\\d{4}(-\\d{4})?)?(/data\\.(csv|tsv|json|csv-metadata\\.json|rdf|ttl))?$");
	
	private int getLimit(HttpServletRequest request) {
		String param = request.getParameter("limit");
		if (param == null) return 250;
		int limit;
		try {
			limit = Integer.parseInt(param);
		} catch (NumberFormatException e) {
			return 250;
		}
		if (limit < 1) return 1;
		if (limit > 1000) return 1000;
		return limit;
	}
	
	private static Map<String, Object> getCSVMetadata(HttpServletRequest request, Legislation.Searchable type, LinkedHashSet<String> components) {
		Map<String, Object> output = new LinkedHashMap<>();
		output.put("@context", "http://www.w3.org/ns/csvw");
		String url = request.getRequestURI().substring(0, request.getRequestURI().length() - 14);
		if (request.getQueryString() != null)
			url += "?" + request.getQueryString();
		output.put("url", url);
		Map<String, Object> notes = new LinkedHashMap<>();
		notes.put("type", type.name());
		notes.put("components", components);
		output.put("notes", notes);
		Map<String, Object> tableSchema = new LinkedHashMap<>();
		List<Map<String, Object>> columns = new ArrayList<>();
		Map<String, Object> column1 = new LinkedHashMap<>();
		column1.put("titles", "ngram");
		column1.put("datatype", "string");
		column1.put("dc:description", "the ngram containing one or more of the given components");
		columns.add(column1);
		Map<String, Object> column2 = new LinkedHashMap<>();
		column2.put("titles", "count");
		column2.put("datatype", "integer");
		column2.put("dc:description", "the number of times the ngram appears");
		columns.add(column2);
		tableSchema.put("columns", columns);
		tableSchema.put("primaryKey", "ngram");
		output.put("tableSchema", tableSchema);
		Map<String, Object> dialect = new LinkedHashMap<>();
		dialect.put("delimiter", ",");
		dialect.put("header", true);
		dialect.put("quoteChar", "\"");
		dialect.put("doubleQuote", true);
		output.put("dialect", dialect);
		return output;
	}
	
	@Override
	public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		
		if ("/search".equalsIgnoreCase(request.getRequestURI())) {
			request.getRequestDispatcher("/WEB-INF/search.html").forward(request, response);
			return;
		} else if (!pattern.matcher(request.getRequestURI()).matches()) {
			throw new BadRequestException("bad request");
		}
		
		Legislation.Searchable legType = getLegType(request);
		Ngrams.Type ngType = getNgramsType(request);
		LinkedHashSet<String> components = getNgrams(request, ngType);
		boolean beginning = "true".equalsIgnoreCase(request.getParameter("beginning"));
		
		if (request.getRequestURI().toLowerCase().endsWith("data.csv-metadata.json")) {
			response.setContentType("application/json; charset=utf-8");
			JSON json = new JSON(response.getWriter());
			Map<String, Object> output = getCSVMetadata(request, legType, components);
			json.write(output);
			return;
		}
//		int limit = getLimit(request);
		SortedMap<Integer, LinkedHashMap<String, Long>> ngrams = uk.gov.legislation.research.ngrams.hadoop.hbase.Search1.get(HBase.config(), (Legislation.Type) legType, components, ngType, beginning);
		ngrams = SearchFormatter.untokenize(ngrams);

		if (request.getRequestURI().toLowerCase().endsWith("/data.json")) {
			response.setContentType("application/json; charset=utf-8");
			JSON.write(response.getWriter(), ngrams);

		} else if (request.getRequestURI().toLowerCase().endsWith("/data.csv")) {
			response.setContentType("text/csv");
			CSV csv = new CSV(response.getWriter());
			csv.writeln(Arrays.asList("ngram", "count"));
			for (LinkedHashMap<String, Long> counts : ngrams.values()) {
				for (Entry<String, Long> count : counts.entrySet()) {
					csv.writeln(Arrays.asList(count.getKey(), count.getValue()));
				}
			}
		} else if (request.getRequestURI().toLowerCase().endsWith("/data.tsv")) {
			response.setContentType(TSV.contentType);
			PrintWriter printer = response.getWriter();
			TSV.line(printer, Arrays.asList("ngram", "count"));
			for (LinkedHashMap<String, Long> counts : ngrams.values()) {
				for (Entry<String, Long> count : counts.entrySet()) {
					TSV.line(printer, Arrays.asList(count.getKey(), count.getValue()));
				}
			}
		} else if (request.getRequestURI().toLowerCase().endsWith("/data.rdf")) {
			response.setContentType("application/rdf+xml");
			DataCube qb = SearchFormatter.toDataCube(components, ngrams);
			qb.write(response.getOutputStream(), DataCube.Format.XML);
		} else if (request.getRequestURI().toLowerCase().endsWith("/data.ttl")) {
			response.setContentType("text/turtle");
			DataCube qb = SearchFormatter.toDataCube(components, ngrams);
			qb.write(response.getOutputStream(), DataCube.Format.Turtle);
		} else {
			request.setAttribute("legType", legType.name());
			request.setAttribute("components", request.getParameter("words"));
			request.setAttribute("ngType", ngType.name());
			request.setAttribute("ngrams", ngrams);
			request.getRequestDispatcher("/WEB-INF/search.jsp").forward(request, response);		
		}
	}

}
