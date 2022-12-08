package uk.gov.legislation.research.qb;

import java.io.OutputStream;

import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.vocabulary.DCTerms;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.apache.jena.vocabulary.XSD;

public class DataCube {
	
	public static enum Format { Turtle, XML }
	
	private static final String prefix = "qb";
	static final String uri = "http://purl.org/linked-data/cube#";
	
	private final Model model;
	
	public DataCube(String prefix, String ns) {
		model = new Model(ns);
		model.setNsPrefix(DataCube.prefix, DataCube.uri);
		model.setNsPrefix("rdf", RDF.getURI());
		model.setNsPrefix("rdfs", RDFS.getURI());
		model.setNsPrefix("dct", DCTerms.getURI());
		model.setNsPrefix("xsd", XSD.getURI());
		model.setNsPrefix(prefix, ns);
	}
	
	public DataCube addTitle(String title) {
		model.getDataset().addProperty(DCTerms.title, title);
		return this;
	}
	public DataCube addLabel(String label) {
		model.getDataset().addProperty(RDFS.label, label);
		return this;
	}
	public DataCube addDescription(String description) {
		model.getDataset().addProperty(DCTerms.description, description);
		return this;
	}
	public DataCube addComment(String comment) {
		model.getDataset().addProperty(RDFS.comment, comment);
		return this;
	}
	
	private int dimensions = 0;
	public Dimension addDimention(String name, Resource range) {
		Dimension dim = new Dimension(model, name, range);
		dimensions += 1;
		Resource entry = model.createResource();
		entry.addProperty(ResourceFactory.createProperty(DataCube.uri + "dimension"), ResourceFactory.createResource(dim.getURI()));
		entry.addProperty(ResourceFactory.createProperty(DataCube.uri + "order"), Integer.toString(dimensions), XSDDatatype.XSDinteger);
		model.getDSD().addProperty(ResourceFactory.createProperty(DataCube.uri + "component"), entry);
		return dim;
	}
	
	public Attribute addAttribute(String name) {
		Attribute attr = new Attribute(model, name);
		Resource entry = model.createResource();
		entry.addProperty(ResourceFactory.createProperty(DataCube.uri + "attribute"), ResourceFactory.createResource(attr.getURI()));
		model.getDSD().addProperty(ResourceFactory.createProperty(DataCube.uri + "component"), entry);
		return attr;
	}

	public Measure addMeasure(String name) {
		Measure msre = new Measure(model, name);
		Resource entry = model.createResource();
		entry.addProperty(ResourceFactory.createProperty(DataCube.uri + "measure"), ResourceFactory.createResource(msre.getURI()));
		model.getDSD().addProperty(ResourceFactory.createProperty(DataCube.uri + "component"), entry);
		return msre;
	}
	
	private int observations = 0;
	public Observation addObservation() {
		observations += 1;
		String name = "o" + observations;
		return new Observation(model, name);
	}
	
	public void write(OutputStream out, Format format) {
		if (format.equals(Format.XML))
			model.write(out, "RDF/XML");
		else
			new TurtleWriter().write(model, out);
	}

}
