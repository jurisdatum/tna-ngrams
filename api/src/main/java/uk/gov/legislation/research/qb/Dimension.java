package uk.gov.legislation.research.qb;

import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;

public class Dimension {
	
	static final String uri = DataCube.uri + "DimentionProperty";
	
	private final Resource resource;
	
	Dimension(Model model, String name, Resource range) {
		resource = model.createResource(model.ns + name);
		resource.addProperty(RDF.type, RDF.Property);
		resource.addProperty(RDF.type, ResourceFactory.createResource(uri));
		resource.addProperty(RDFS.range, range);
	}
	
	String getURI() {
		return resource.getURI();
	}
	
	public Dimension setLabel(String label) {
		resource.addProperty(RDFS.label, label);
		return this;
	}

}
