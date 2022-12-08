package uk.gov.legislation.research.ngrams.api;

import uk.gov.legislation.research.Legislation;
import uk.gov.legislation.research.ngrams.Ngrams;
import uk.gov.legislation.research.ngrams.api.DateRange.MonthRange;
import edu.stanford.nlp.util.StringUtils;
import org.apache.hadoop.conf.Configuration;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.*;

import static uk.gov.legislation.research.ngrams.api.Explorer.*;

class Counts {

	static void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
		
		Legislation.Searchable legType = getLegType(request);
		DateRange dates = getDateRange(request);
		Ngrams.Type ngType = getNgramsType(request);
		LinkedHashSet<String> ngrams = getNgrams(request, ngType);
		
		String filename = request.getRequestURI().substring(request.getRequestURI().lastIndexOf('/')+1).toLowerCase();
		
		response.addHeader("Access-Control-Allow-Origin", "*");

		if (filename.equals("data.csv-metadata.json")) {
			response.setContentType("application/json; charset=utf-8");
			String url = request.getRequestURI().substring(0, request.getRequestURI().length() - 14);
			if (request.getQueryString() != null)
				url += "?" + request.getQueryString();
//			if (dates instanceof MonthRange) {
//				SortedMap<Integer, SortedSet<Integer>> months = ((MonthRange) dates).getMonths();
//				MonthlyCounts.csvMetadata(legType, months, ngrams, ngType, response.getWriter(), url);
//			} else {
				SortedSet<Integer> years = dates.getYears();
				CountsFormatter.csvMetadata(legType, years, ngrams, ngType, response.getWriter(), url);
//			}
			return;
		}

		uk.gov.legislation.research.ngrams.Counts counts;
		Configuration conf = HBase.config();
		LinkedHashMap<String, NavigableMap<Short, Integer>> results = uk.gov.legislation.research.ngrams.hadoop.hbase.Counts.getCounts(conf, (Legislation.Type) legType, ngrams, ngType);
		counts = new CountsFormatter((Legislation.Type) legType, ngType, ngrams, results, dates);

		switch (filename) {
			case "data.json":
				response.setContentType("application/json; charset=utf-8");
				counts.json(response.getWriter());
				break;
			case "data.csv":
				response.setContentType("text/csv");
				counts.csv(response.getWriter());
				break;
			case "data.tsv":
				response.setContentType(TSV.contentType);
				counts.tsv(response.getWriter());
				break;
			case "data.rdf":
				response.setContentType("application/rdf+xml");
				counts.rdf(response.getOutputStream());
				break;
			case "data.ttl":
				response.setContentType("text/turtle");
				counts.turtle(response.getOutputStream());
				break;
			default:
				request.setAttribute("data", counts.json());
				request.setAttribute("ngrams", StringUtils.join(ngrams, ", "));
				request.setAttribute("dates", dates.toString());
				String jsp;
				if (dates instanceof MonthRange)
					jsp = "/WEB-INF/monthlybar.jsp";
				else if (dates.count() <= 3)
					jsp = "/WEB-INF/bargraph.jsp";
				else
					jsp = "/WEB-INF/graph.jsp";
				request.getRequestDispatcher(jsp).forward(request, response);
		}
	}
}
