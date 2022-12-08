package uk.gov.legislation.research;


import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;

import net.sf.saxon.s9api.Axis;
import net.sf.saxon.s9api.DocumentBuilder;
import net.sf.saxon.s9api.Processor;
import net.sf.saxon.s9api.SaxonApiException;
import net.sf.saxon.s9api.XPathCompiler;
import net.sf.saxon.s9api.XPathExecutable;
import net.sf.saxon.s9api.XPathSelector;
import net.sf.saxon.s9api.XdmItem;
import net.sf.saxon.s9api.XdmNode;
import net.sf.saxon.s9api.XdmValue;

public class Xml {
	
	static final Processor processor = new Processor(false);
	
	final XdmNode root;
	private final XPathCompiler compiler;
	
	private Xml(XdmNode node, Map<String, String> namespaces) {
		root = node;
		compiler = processor.newXPathCompiler();
		for (Entry<String, String> namespace : namespaces.entrySet())
			compiler.declareNamespace(namespace.getKey(), namespace.getValue());
	}
	
	private static void addNamespaces(XdmNode node, Map<String, String> accumulator) {
		node.axisIterator(Axis.NAMESPACE).forEachRemaining(namespace -> {
			accumulator.put(((XdmNode) namespace).getUnderlyingNode().getLocalPart(), namespace.getStringValue());
		});
		node.axisIterator(Axis.CHILD).forEachRemaining(child -> {
			addNamespaces((XdmNode) child, accumulator);
		});
	}
	private static Map<String, String> getNamespaces(XdmNode root) {
		Map<String, String> namespaces = new LinkedHashMap<>();
		addNamespaces(root, namespaces);
		return namespaces;
	}

	Xml(XdmNode node) {
		this(node, getNamespaces(node));
	}
	
	private static XdmNode parse(InputStream xml) {
		DocumentBuilder builder = processor.newDocumentBuilder();
		Source source = new StreamSource(xml);
		XdmNode node;
		try {
			node = builder.build(source);
		} catch (SaxonApiException e) {
			throw new RuntimeException("error parsing document", e);
		}
		return node;
	}

	protected Xml(InputStream xml, Map<String, String> namespaces) {
		this(parse(xml), namespaces);
	}

	public Xml(InputStream xml) {
		this(parse(xml));
	}
		
	public XdmValue xpath(String expression) {
		XPathExecutable exec;
		try {
			exec = compiler.compile(expression);
		} catch (SaxonApiException e) {
			throw new RuntimeException("error compiling xpath expression", e);
		}
		XPathSelector selector = exec.load();
		try {
			selector.setContextItem(root);
		} catch (SaxonApiException e) {
			throw new RuntimeException("error setting context item", e);
		}
		XdmValue result;
		try {
			result = selector.evaluate();
		} catch (SaxonApiException e) {
			throw new RuntimeException("error evaluating xpath expression", e);
		}
		return result;
	}

	public XdmItem xpath1(String expression) {
		XPathExecutable exec;
		try {
			exec = compiler.compile(expression);
		} catch (SaxonApiException e) {
			throw new RuntimeException("error compiling xpath expression", e);
		}
		XPathSelector selector = exec.load();
		try {
			selector.setContextItem(root);
		} catch (SaxonApiException e) {
			throw new RuntimeException("error setting context item", e);
		}
		XdmItem result;
		try {
			result = selector.evaluateSingle();
		} catch (SaxonApiException e) {
			throw new RuntimeException("error evaluating xpath expression", e);
		}
		return result;
	}
	
	@Override
	public String toString() {
		return root.toString();
	}

}
