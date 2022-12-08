package uk.gov.legislation.research.qb;

import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.ResIterator;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.rdf.model.impl.ModelCom;
import org.apache.jena.vocabulary.RDF;

class Model extends ModelCom {

	final String ns;
	private final Resource dataset;
	private final Resource dsd;
	
	Resource getDataset() {
		return dataset;
	}
	Resource getDSD() {
		return dsd;
	}
	
	Model(String ns) {
		super(org.apache.jena.graph.Factory.createGraphMem());
		this.ns = ns;
		dataset = createResource(ns + "dataset");
		dataset.addProperty(RDF.type, ResourceFactory.createResource(DataCube.uri + "DataSet"));
		dsd = createResource(ns + "dsd");
		dsd.addProperty(RDF.type, ResourceFactory.createResource(DataCube.uri + "DataStructureDefinition"));
		Property structure = ResourceFactory.createProperty(DataCube.uri + "structure");
		dataset.addProperty(structure, dsd);
	}
	
	ResIterator getDimensions() {
		Resource dimension = ResourceFactory.createResource(Dimension.uri);
		return listSubjectsWithProperty(RDF.type, dimension);
	}

	ResIterator getMeasures() {
		Resource measure = ResourceFactory.createResource(Measure.uri);
		return listSubjectsWithProperty(RDF.type, measure);
	}

	ResIterator getAttributes() {
		Resource attribute = ResourceFactory.createResource(Attribute.uri);
		return listSubjectsWithProperty(RDF.type, attribute);
	}

	ResIterator getObservations() {
		Resource observation = ResourceFactory.createResource(Observation.uri);
		return listSubjectsWithProperty(RDF.type, observation);
	}

}
