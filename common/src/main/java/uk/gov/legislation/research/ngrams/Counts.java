package uk.gov.legislation.research.ngrams;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Writer;

public interface Counts {
	
	public void json(Writer writer);
	
	public String json();
	
	public void csv(PrintWriter writer);
	
	public void tsv(PrintWriter writer);
	
	public void rdf(OutputStream output);

	public void turtle(OutputStream output);

}
