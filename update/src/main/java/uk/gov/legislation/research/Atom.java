package uk.gov.legislation.research;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import net.sf.saxon.s9api.XdmItem;
import net.sf.saxon.s9api.XdmValue;

public class Atom extends Xml {
	
	private static final Map<String, String> namespaces = new HashMap<>();
	static {
		namespaces.put("", "http://www.w3.org/2005/Atom");
		namespaces.put("leg", "http://www.legislation.gov.uk/namespaces/legislation");
		namespaces.put("ukm", "http://www.legislation.gov.uk/namespaces/metadata");
	}
		
	private Atom(InputStream atom) {
		super(atom, namespaces);
	}
	
	public static Atom get(String url) throws IOException {
		HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
		connection.connect();
		Atom atom;
		try {
			InputStream stream = connection.getInputStream();
			try {
				atom = new Atom(stream);
			} finally {
				stream.close();
			}
		} finally {
			connection.disconnect();
		}
		return atom;
	}
	
	public static Atom get(Legislation.Type type, int year, int page) throws IOException {
		String url = "http://legislation.data.gov.uk/" + type.name() + "/" + year + "/data.feed?page=" + page;
		return get(url);
	}
	
	public XdmValue getIds() {
		return xpath("/feed/entry/id");
	}
	
	public boolean hasNextPage() {
		XdmValue next = xpath("/feed/link[@rel='next']/@href");
		return next != null && next.size() != 0;
	}
	
	private static Atom try100(String url) {
		for (int i = 1; i <= 100; i++) {
			try {
				return get(url);
			} catch (IOException e) {
			}
		}
		throw new RuntimeException();
	}

	
	public static Set<String> getIds(Legislation.Type type, Integer startYear) {
		
		Set<String> ids = new HashSet<>();
		
		Atom typeFeed = try100("http://legislation.data.gov.uk/" + type.name() + "/data.feed");
		for (XdmItem yearAttr : typeFeed.xpath("/feed/leg:facets/leg:facetYears/leg:facetYear/@year")) {
			int year = Integer.parseInt(yearAttr.getStringValue());
			if (startYear != null && year < startYear)
				continue;
			int page = 1;
			while (true) {
				Atom feed = try100("http://legislation.data.gov.uk/" + type.name() + "/" + year + "/data.feed?page=" + page);
				for (XdmItem entry : feed.xpath("/feed/entry/id")) {
					String id = entry.getStringValue().substring(33);
					ids.add(id);
				}
				XdmValue nextAttr = feed.xpath("/feed/link[@rel='next']/@href");
				if (nextAttr == null || nextAttr.size() == 0)
					break;
				String href = nextAttr.itemAt(0).getStringValue();
				page = Integer.parseInt(href.substring(href.lastIndexOf('=') + 1));
			}
		}
		return ids;
	}

	public static Set<String> getIdsForYear(Legislation.Type type, Integer year) throws IOException {
		Set<String> ids = new HashSet<>();
		int page = 1;
		while (true) {
			Atom feed = get("https://www.legislation.gov.uk/" + type.name() + "/" + year + "/data.feed?page=" + page);
			for (XdmItem entry : feed.xpath("/feed/entry/id")) {
				String id = entry.getStringValue().substring(33);
				ids.add(id);
			}
			XdmValue nextAttr = feed.xpath("/feed/link[@rel='next']/@href");
			if (nextAttr == null || nextAttr.size() == 0)
				break;
			String href = nextAttr.itemAt(0).getStringValue();
			page = Integer.parseInt(href.substring(href.lastIndexOf('=') + 1));
		}
		return ids;
	}


}
