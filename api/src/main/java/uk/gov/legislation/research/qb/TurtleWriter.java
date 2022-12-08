package uk.gov.legislation.research.qb;

import java.io.OutputStream;

import org.apache.jena.n3.N3JenaWriterPP;
import org.apache.jena.rdf.model.ResIterator;
import org.apache.jena.rdf.model.Resource;

public class TurtleWriter {
	
	private class DataCubeTurtleWriter extends N3JenaWriterPP {
		
		@Override
		protected void writeModel(org.apache.jena.rdf.model.Model jenaModel) {
			Model model = (Model) jenaModel;
			out.println("# -- Data Set --------------------------------------------");
			out.println();
	        Resource dataset = model.getDataset();
	        writeOneGraphNode(dataset);
			out.println();
			out.println("# -- Data structure definition ---------------------------");
			out.println();
	        Resource dsd = model.getDSD();
	        writeOneGraphNode(dsd);
			out.println();
			out.println("# -- Dimensions and measures -----------------------------");
			out.println();
	        ResIterator dimensions = model.getDimensions();
	        while (dimensions.hasNext()) {
	            Resource dimension = dimensions.nextResource();
	            writeOneGraphNode(dimension);
	        }
	        ResIterator measures = model.getMeasures();
	        while (measures.hasNext()) {
	            Resource measure = measures.nextResource();
	            writeOneGraphNode(measure);
	        }
	        ResIterator attributes = model.getAttributes();
	        while (attributes.hasNext()) {
	            Resource attribute = attributes.nextResource();
	            writeOneGraphNode(attribute);
	        }
			out.println();
			out.println("# -- Observations ----------------------------------------");
			out.println();
	        ResIterator observations = model.getObservations();
	        while (observations.hasNext()) {
	            Resource observation = observations.nextResource();
	            writeOneGraphNode(observation);
	        }
	    }
	}
	
	private final DataCubeTurtleWriter writer = new DataCubeTurtleWriter();
	
	void write(Model model, OutputStream out) {
		writer.write(model, out, null);
	}

}
