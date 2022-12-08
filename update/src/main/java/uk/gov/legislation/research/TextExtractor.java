package uk.gov.legislation.research;

import java.io.InputStream;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import net.sf.saxon.s9api.XdmNode;

public class TextExtractor {
	
	public enum Option { Titles, Explanatory_Notes, Commentaries;
		public String toString() { return super.toString().replace('_', '-').toLowerCase(); }
	}
	
	private static InputStream stylesheet() {
		return TextExtractor.class.getResourceAsStream("text.xsl");
	}
	
	private final Xslt transform;
	private final Map<String, Object> parameters = new HashMap<>();
	
	public TextExtractor(EnumMap<Option, Boolean> options) {
		transform = new Xslt(stylesheet());
		if (options == null) return;
		for (Entry<Option, Boolean> option : options.entrySet())
			parameters.put(option.getKey().toString(), option.getValue());
	}
	public TextExtractor() {
		this(null);
	}
	
	public String extract(Clml clml) {
		return transform.transformToText(clml, parameters);
	}
	public String extract(XdmNode clml) {
		return transform.transformToText(clml, parameters);
	}

}
