package uk.gov.legislation.research.qb;

import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.vocabulary.RDF;

public class Observation {

	static final String uri = DataCube.uri + "Observation";
	
	private final Resource resource;
	
	Observation(Model model, String name) {
		resource = model.createResource(model.ns + name);
		resource.addProperty(RDF.type, ResourceFactory.createResource(uri));
		Property dataset = ResourceFactory.createProperty(DataCube.uri + "dataSet");
		resource.addProperty(dataset, model.getDataset());
	}
	
	public Observation setDimension(Dimension dimension, String value) {
		Property property = ResourceFactory.createProperty(dimension.getURI());
		resource.addProperty(property, value);
		return this;
	}
	public Observation setDimension(Dimension dimension, int value) {
		Property property = ResourceFactory.createProperty(dimension.getURI());
		resource.addProperty(property, Integer.toString(value), XSDDatatype.XSDinteger);
		return this;
	}

	private Observation setAttribute(Attribute attr, String value, XSDDatatype type) {
		Property property = ResourceFactory.createProperty(attr.getURI());
		resource.addProperty(property, value, type);
		return this;
	}
	public Observation setAttribute(Attribute attr, String value) {
		return setAttribute(attr, value, XSDDatatype.XSDstring);
	}
	public Observation setAttribute(Attribute attr, int value) {
		return setAttribute(attr, Integer.toString(value), XSDDatatype.XSDinteger);
	}
	public Observation setAttribute(Attribute attr, double value) {
		return setAttribute(attr, Double.toString(value), XSDDatatype.XSDdouble);
	}

	private Observation setMeasure(Measure measure, String value, XSDDatatype type) {
		Property property = ResourceFactory.createProperty(measure.getURI());
		resource.addProperty(property, value, type);
		return this;
	}
	public Observation setMeasure(Measure measure, int value) {
		return setMeasure(measure, Integer.toString(value), XSDDatatype.XSDinteger);
	}
	public Observation setMeasure(Measure measure, long value) {
		return setMeasure(measure, Long.toString(value), XSDDatatype.XSDlong);
	}

}
