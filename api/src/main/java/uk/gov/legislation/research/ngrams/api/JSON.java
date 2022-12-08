package uk.gov.legislation.research.ngrams.api;

import org.json.JSONException;
import org.json.JSONWriter;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class JSON {
	
	private static void write(JSONWriter writer, Object value) throws JSONException {
		if (value instanceof Map)
			object(writer, (Map<?, ?>) value);
		else if (value instanceof List)
			array(writer, (List<?>) value);
		else
			writer.value(value);
	}
	
	private static void object(JSONWriter writer, Map<?, ?> map) throws JSONException {
		writer.object();
		for (Entry<?, ?> entry : map.entrySet()) {
			writer.key(entry.getKey().toString());
			write(writer, entry.getValue());
		}
		writer.endObject();
	}

	private static void array(JSONWriter writer, List<?> list) throws JSONException {
		writer.array();
		for (Object value : list) write(writer, value);
		writer.endArray();
	}

	static void write(PrintWriter out, Object value) {
		JSONWriter writer = new JSONWriter(out);
		try {
			write(writer, value);
		} catch (JSONException e) {
			throw new RuntimeException(e);
		}
	}
		
	private final JSONWriter writer;
	
	public JSON(Writer writer) {
		this.writer = new JSONWriter(writer);
	}

	void write(List<? extends Map<String, Object>> list) {
		try {
			writer.array();
			for (Map<String, Object> map : list) {
				writer.object();
				for (Entry<String, Object> entry : map.entrySet()) {
					writer.key(entry.getKey());
					writer.value(entry.getValue());
				}
				writer.endObject();
			}
			writer.endArray();
		} catch (JSONException e) {
			throw new RuntimeException(e);
		}
	}

	public void write(Map<String, ? extends Object> map) {
		try {
			write(writer, map);
		} catch (JSONException e) {
			throw new RuntimeException(e);
		}
	}

	static String stringify(List<? extends Map<String, Object>> list) {
		StringWriter writer = new StringWriter();
		new JSON(writer).write(list);
		return writer.toString();
	}
}
