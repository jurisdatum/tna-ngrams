package uk.gov.legislation.research;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;


import net.sf.saxon.s9api.XdmItem;
import net.sf.saxon.s9api.XdmValue;


public class Clml extends Xml {
	
	private static final Map<String, String> namespaces = new HashMap<>();
	static {
		namespaces.put("", "http://www.legislation.gov.uk/namespaces/legislation");
		namespaces.put("leg", "http://www.legislation.gov.uk/namespaces/legislation");
		namespaces.put("ukm", "http://www.legislation.gov.uk/namespaces/metadata");
		namespaces.put("dc", "http://purl.org/dc/elements/1.1/");
		namespaces.put("dct", "http://purl.org/dc/terms/");
		namespaces.put("atom", "http://www.w3.org/2005/Atom");
		namespaces.put("html", "http://www.w3.org/1999/xhtml");
		namespaces.put("math", "http://www.w3.org/1998/Math/MathML");
	}
	
	public Clml(InputStream xml) {
		super(xml, namespaces);
	}
	
	@SuppressWarnings("serial")
	public static class NoDocumentException extends RuntimeException {
		public final int responseCode;
		public NoDocumentException(int responseCode) { this.responseCode = responseCode; }
	}
	@SuppressWarnings("serial")
	public static class NoClmlException extends RuntimeException {}

	private static byte[] toByteArray(InputStream is) throws IOException {
		ByteArrayOutputStream buffer = new ByteArrayOutputStream();
		int nRead;
		byte[] data = new byte[4];
		while ((nRead = is.read(data, 0, data.length)) != -1) {
			buffer.write(data, 0, nRead);
		}
		buffer.flush();
		return buffer.toByteArray();
	}
	
	private static byte[] getRaw(URL url, boolean followRedirects) {
		HttpURLConnection connection;
		try {
			connection = (HttpURLConnection) url.openConnection();
		} catch (IOException e) {
			throw new RuntimeException("error opening url connection", e);
		}
		connection.setConnectTimeout(15000);
		connection.setReadTimeout(60000);
		connection.addRequestProperty("User-Agent","Mangiafico");
		connection.setInstanceFollowRedirects(followRedirects);
		try {
			connection.connect();
		} catch (IOException e) {
			throw new RuntimeException("error establishing http connection", e);
		}
		try {
			int responseCode;
			try {
				responseCode = connection.getResponseCode();
			} catch (IOException e) {
				throw new RuntimeException("error getting http response code", e);
			}
			if (responseCode != 200)
				throw new NoDocumentException(responseCode);
			if (!connection.getHeaderField("Content-Type").startsWith("application/xml"))
				throw new NoClmlException();
			byte[] clml;
			InputStream stream;
			try {
				stream = connection.getInputStream();
			} catch (IOException e) {
				throw new RuntimeException("error opening input stream", e);
			}
			try {
				clml = toByteArray(stream);
			} catch (IOException e) {
				throw new RuntimeException("error reading input stream", e);
			} finally {
				try {
					stream.close();
				} catch (IOException e) {
//					throw new RuntimeException("error closing input stream", e);
				}
			}
			return clml;
		} finally {
			connection.disconnect();
		}
	}

	public static byte[] getRaw(String id, String version) {
		StringBuilder url = new StringBuilder("https://www.legislation.gov.uk/");
		url.append(id);
		if (version != null) {
			url.append('/');
			url.append(version);
		}
		url.append("/data.xml");
		URL u;
		try {
			u = new URL(url.toString());
		} catch (MalformedURLException e) {
			throw new RuntimeException(e);
		}
		return getRaw(u, version == null);
	}
	public static byte[] getLatestRaw(String id) {
		return getRaw(id, null);
	}

	public static Clml get(String id, String version) {
		throw new RuntimeException();
	}
	public static Clml getLatest(String id) {
		return get(id, null);
	}

		public boolean hasBody() {
		if (xpath("/Legislation/*/Body").size() > 0)
			return true;
		if (xpath("/Legislation/EURetained/EUBody").size() > 0)
			return true;
		return false;
	}
	
	public boolean isPrimary() {
		return xpath1("/Legislation/ukm:Metadata/ukm:*/ukm:DocumentClassification/ukm:DocumentCategory/@Value")
			.getStringValue().equalsIgnoreCase("primary");
	}
	
	public Integer getYear() {
		XdmItem year = xpath1("/Legislation/ukm:Metadata/ukm:*/ukm:Year/@Value");
		if (year == null)
			return null;
		return Integer.parseInt(year.getStringValue());
	}
	
	public String getTitle() {
		return xpath1("/Legislation/ukm:Metadata/dc:title").getStringValue();
	}
	
	public Date getDate() {
		XdmItem date;
		if (isPrimary())
			date = xpath1("/Legislation/ukm:Metadata/ukm:PrimaryMetadata/ukm:EnactmentDate/@Date");
		else
			date = xpath1("/Legislation/ukm:Metadata/ukm:SecondaryMetadata/ukm:Made/@Date");
		if (date == null)
			return null;
		DateFormat format = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
		try {
			return format.parse(date.getStringValue());
		} catch (ParseException e) {
			return null;
		}
	}
	
	public List<String> getVersions() {
		XdmValue titles = xpath("/Legislation/ukm:Metadata/atom:link[@rel='http://purl.org/dc/terms/hasVersion']/@title");
		List<String> versions = new ArrayList<>(titles.size());
		for (XdmItem title : titles)
			versions.add(title.getStringValue());
		if (versions.isEmpty()) {
			String[] parts = xpath1("/Legislation/ukm:Metadata/atom:link[@rel='self']/@href").getStringValue().split("/");
			if (parts.length >= 2 && (parts[parts.length-2].equals("enacted") || parts[parts.length-2].equals("made")))
				versions.add(parts[parts.length-2]);
		}
		return versions;		
	}

}
