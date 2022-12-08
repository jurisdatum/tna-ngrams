package uk.gov.legislation.research.ngrams.api;

import java.io.PrintWriter;
import java.util.Iterator;

public class TSV {
	
	public static final String contentType = "text/tab-separated-values";
	
	public static final char delimiter = '\t';

	public static void line(PrintWriter printer, Iterable<?> values) {
		Iterator<?> iterator = values.iterator();
		if (iterator.hasNext()) {
			printer.print(iterator.next());
		}
		while (iterator.hasNext()) {
			printer.print(delimiter);
			printer.print(iterator.next());
		}
		printer.println();
	}

	void lines(PrintWriter printer, Iterable<Iterable<?>> lines) {
		for (Iterable<?> line : lines)
			line(printer, line);
	}

}
