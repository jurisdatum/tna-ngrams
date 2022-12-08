package uk.gov.legislation.research.ngrams.api;

import java.io.PrintWriter;
import java.util.Iterator;


public class CSV {
	
	private static void write(PrintWriter writer, String value) {
		if (value.contains("\"")) {
			writer.print("\"");
			writer.print(value.replaceAll("\"", "\"\""));
			writer.print("\"");				 
		} else if (value.contains(",")) {
			writer.print("\"");
			writer.print(value);
			writer.print("\"");
		} else {
			writer.print(value);
		}
	}

	private static void write(PrintWriter writer, Object value) {
		if (value instanceof String)
			write(writer, (String) value);
		else if (value instanceof Number)
			writer.print(value);
		else
			write(writer, value.toString());
	}

	private final PrintWriter writer;
	
	public CSV(PrintWriter writer) {
		this.writer = writer;
	}
	
	private void write(Object value) {
		write(writer, value);
	}

	public void writeln(Iterable<?> values) {
		Iterator<?> iterator = values.iterator();
		if (iterator.hasNext()) {
			write(iterator.next());
		}
		while (iterator.hasNext()) {
			writer.print(",");
			write(iterator.next());
		}
		writer.println();
	}

	public void writeln(String value1, String value2, Iterable<?> values) {
		write(value1);
		writer.print(",");
		write(value2);
		Iterator<?> iterator = values.iterator();
		while (iterator.hasNext()) {
			writer.print(",");
			write(iterator.next());
		}
		writer.println();
	}
}
