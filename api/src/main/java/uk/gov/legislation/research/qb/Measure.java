package uk.gov.legislation.research.qb;

import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;

public class Measure {
	
	static final String uri = DataCube.uri + "MeasureProperty";
	
	private final Resource resource;
	
	Measure(Model model, String name) {
		resource = model.createResource(model.ns + name);
		resource.addProperty(RDF.type, ResourceFactory.createResource(uri));
		resource.addProperty(RDF.type, RDF.Property);
	}
	
	String getURI() {
		return resource.getURI();
	}
	
	public Measure setLabel(String label) {
		resource.addProperty(RDFS.label, label);
		return this;
	}

}
