package uk.gov.legislation.research;

import java.io.InputStream;
import java.io.StringWriter;
import java.util.Map;
import java.util.Map.Entry;

import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;

import net.sf.saxon.s9api.Destination;
import net.sf.saxon.s9api.QName;
import net.sf.saxon.s9api.SaxonApiException;
import net.sf.saxon.s9api.Serializer;
import net.sf.saxon.s9api.XdmAtomicValue;
import net.sf.saxon.s9api.XdmDestination;
import net.sf.saxon.s9api.XdmNode;
import net.sf.saxon.s9api.XdmValue;
import net.sf.saxon.s9api.XsltCompiler;
import net.sf.saxon.s9api.XsltExecutable;
import net.sf.saxon.s9api.XsltTransformer;

public class Xslt {
	
	private static final XsltCompiler compiler = Xml.processor.newXsltCompiler(); 
	
	private final XsltExecutable stylesheet;
	
	public Xslt(InputStream stylesheet) {
		Source source = new StreamSource(stylesheet);
		try {
			this.stylesheet = compiler.compile(source);
		} catch (SaxonApiException e) {
			throw new RuntimeException("error reading xslt file", e);
		}
	}
	
	private void addParameters(XsltTransformer transform, Map<String, Object> parameters) {
		if (parameters == null) return;
		for (Entry<String, Object> parameter : parameters.entrySet()) {
			QName name = new QName(parameter.getKey());
			XdmValue value;
			if (parameter.getValue() instanceof Boolean)
				value = new XdmAtomicValue((Boolean) parameter.getValue());
			else
				value = new XdmAtomicValue(parameter.getValue().toString());
			transform.setParameter(name, value);
		}
	}
	
	private void transform(XdmNode source, Destination destination, Map<String, Object> parameters) {
		XsltTransformer transform = stylesheet.load();
		transform.setInitialContextNode(source);
		addParameters(transform, parameters);
		transform.setDestination(destination);
        try {
			transform.transform();
		} catch (SaxonApiException e) {
			throw new RuntimeException("error performing xslt", e);
		}
	}

	public String transformToText(XdmNode node, Map<String, Object> parameters) {
		StringWriter output = new StringWriter();
		Serializer out = Xml.processor.newSerializer(output);
		transform(node, out, parameters);
		return output.toString();
	}

	public String transformToText(Xml source, Map<String, Object> parameters) {
		return transformToText(source.root, parameters);
	}
	
	public Xml transform(Xml source, Map<String, Object> parameters) {
		XdmDestination result = new XdmDestination();
		transform(source.root, result, parameters);
		return new Xml(result.getXdmNode());
	}

}
