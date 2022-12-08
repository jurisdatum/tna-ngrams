package uk.gov.legislation.research.qb;

import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;

public class Attribute {
	
	static final String uri = DataCube.uri + "AttributeProperty";

	private final Resource resource;
	
	Attribute(Model model, String name) {
		resource = model.createResource(model.ns + name);
		resource.addProperty(RDF.type, RDF.Property);
		resource.addProperty(RDF.type, ResourceFactory.createResource(uri));
	}
	
	String getURI() {
		return resource.getURI();
	}
	
	public Attribute setLabel(String label) {
		resource.addProperty(RDFS.label, label);
		return this;
	}


}
