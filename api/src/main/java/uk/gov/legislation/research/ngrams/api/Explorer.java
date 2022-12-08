package uk.gov.legislation.research.ngrams.api;

import java.io.IOException;
import java.util.Calendar;
import java.util.LinkedHashSet;
import java.util.regex.Pattern;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;

import uk.gov.legislation.research.Legislation;
import uk.gov.legislation.research.ngrams.Globals;
import uk.gov.legislation.research.ngrams.Ngrams;
import uk.gov.legislation.research.ngrams.Ngrams.Type;
import uk.gov.legislation.research.ngrams.api.DateRange.MonthRange;

@WebServlet("/explorer/*")
public class Explorer extends HttpServlet {
		
	static final Pattern counts = Pattern.compile("^/explorer/[a-z]{3,9}(/\\d{4}(-\\d{4})?)?(/data\\.(csv|tsv|json|csv-metadata\\.json|rdf|ttl))?$");
	static final Pattern documents = Pattern.compile("^/explorer/[a-z]{3,9}(/\\d{4}(-\\d{4})?)?/instances(/data\\.(csv|tsv|json|csv-metadata\\.json|rdf|ttl))?$");
	
	@Override
	public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
		if ("/explorer".equalsIgnoreCase(request.getRequestURI()))
			request.getRequestDispatcher("/index.html").forward(request, response);
		else if (counts.matcher(request.getRequestURI()).matches())
			Counts.doGet(request, response);
		else if (documents.matcher(request.getRequestURI()).matches())
			Instances.doGet(request, response);
		else
			throw new BadRequestException("bad request");
	}

	static Legislation.Searchable getLegType(HttpServletRequest request) throws BadRequestException {
		String type = request.getRequestURI().substring(1).split("/")[1];
		try {
			return Legislation.Type.valueOf(type);
		} catch (IllegalArgumentException e) {
			try {
				return Legislation.Group.valueOf(type);
			} catch (IllegalArgumentException e1) {
				throw new BadRequestException("unsupported document type");
			}
		}
	}

	static DateRange getDateRange(HttpServletRequest request) throws BadRequestException {
		
		DateRange dates = new DateRange(Globals.FIRST_YEAR, Calendar.getInstance().get(Calendar.YEAR));
		
		String[] parts = request.getRequestURI().substring(1).split("/");
		if (parts.length > 2 && parts[2].matches("\\d{4}-\\d{4}")) {
			int start = Integer.parseInt(parts[2].substring(0, 4));
			int end = Integer.parseInt(parts[2].substring(5));
			dates = new DateRange(start, end);
		} else if (parts.length > 2 && parts[2].matches("\\d{4}")) {
			int year = Integer.parseInt(parts[2]);
			dates = new DateRange(year, year);
		}
		if (request.getParameter("start") != null) {
			DateTime start;
			try {
				start = ISODateTimeFormat.date().parseDateTime(request.getParameter("start"));
			} catch (IllegalArgumentException e) {
				throw new BadRequestException("start parameter is not a valid date");
			}
			dates = new MonthRange(start.getYear(), start.getMonthOfYear(), dates.end, Calendar.DECEMBER + 1);
		}
		if (request.getParameter("end") != null) {
			DateTime end;
			try {
				end = ISODateTimeFormat.date().parseDateTime(request.getParameter("end"));
			} catch (IllegalArgumentException e) {
				throw new BadRequestException("end parameter is not a valid date");
			}
			if (dates instanceof MonthRange) {
				dates = new MonthRange(dates.start, ((MonthRange) dates).startMonth, end.getYear(), end.getMonthOfYear());
			} else {
				dates = new MonthRange(dates.start, 1, end.getYear(), end.getMonthOfYear());
			}
		}
		if ("month".equalsIgnoreCase(request.getParameter("interval")) && !(dates instanceof MonthRange)) {
			dates = new MonthRange(dates.start, 1, dates.end, 12);
		}
		return dates;
	}

	static Ngrams.Type getNgramsType(HttpServletRequest request) {
		if ("true".equalsIgnoreCase(request.getParameter("stem")))
			return Type.Lemmas;
		else if ("true".equalsIgnoreCase(request.getParameter("case")))
			return Type.Case_Sensitive;
		else
			return Type.Case_Insensitive;
	}
		
	static LinkedHashSet<String> getNgrams(HttpServletRequest request, Ngrams.Type ngType) {
		LinkedHashSet<String> ngrams = new LinkedHashSet<>();
		String words = request.getParameter("words");
		if (words == null)
			return ngrams;
		String[] wordss = words.split("(?<!\\\\),");
		for (int i = 0; i < wordss.length; i++)
			wordss[i] = wordss[i].replace("\\,", ",");
		for (String ngram : wordss)
			ngrams.add(Ngrams.tokenize(ngram, ngType));
		return ngrams;
	}

}
