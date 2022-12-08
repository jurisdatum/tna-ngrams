package uk.gov.legislation.research.ngrams.api;

import uk.gov.legislation.research.Legislation;
import uk.gov.legislation.research.ngrams.Ngrams;
import org.apache.hadoop.conf.Configuration;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.NavigableMap;
import java.util.stream.Collectors;

import static uk.gov.legislation.research.ngrams.api.Explorer.*;

public class Instances {
	
	static void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
		
		Legislation.Searchable legType = getLegType(request);
		DateRange dates = getDateRange(request);
		Ngrams.Type ngType = getNgramsType(request);
		LinkedHashSet<String> ngrams = getNgrams(request, ngType);

		String filename = request.getRequestURI().substring(request.getRequestURI().lastIndexOf('/')+1).toLowerCase();
		
		response.addHeader("Access-Control-Allow-Origin", "*");

		if (filename.equalsIgnoreCase("data.csv-metadata.json")) {
			response.setContentType("application/json; charset=utf-8");
			String url = request.getRequestURI().substring(0, request.getRequestURI().length() - 14);
			if (request.getQueryString() != null)
				url += "?" + request.getQueryString();
//			if (dates instanceof MonthRange)
//				DocumentCounts.csvMetadata(legType, ((MonthRange) dates).getMonths(), ngrams, ngType, response.getWriter(), url);
//			else
				InstancesFormatter.csvMetadata(legType, dates.getYears(), ngrams, ngType, response.getWriter(), url);
			return;
		}

		Configuration conf = HBase.config();
		LinkedHashMap<String, NavigableMap<Short, LinkedHashMap<String, Integer>>> results = uk.gov.legislation.research.ngrams.hadoop.hbase.Counts.getDocumentCounts(conf, (Legislation.Type) legType, ngrams, ngType);
		InstancesFormatter counts = new InstancesFormatter(results, dates);

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
				response.setContentType("text/csv");
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
				request.setAttribute("caption", String.join(", ", ngrams.stream().map(Ngrams::untokenize).collect(Collectors.toList())));
				request.setAttribute("ngrams", ngrams.stream().map(ng -> Ngrams.untokenize(ng)).collect(Collectors.toList()));
				request.setAttribute("years", dates.toString());
				request.setAttribute("titles", counts.titles);
				request.setAttribute("instances", counts.counts);
				request.getRequestDispatcher("/WEB-INF/instances.jsp").forward(request, response);		
		}

	}

}
