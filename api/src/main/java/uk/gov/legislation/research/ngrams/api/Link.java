package uk.gov.legislation.research.ngrams.api;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

import uk.gov.legislation.research.ngrams.Ngrams;

public class Link {
	
	public static String link(String legType, String ngram, String ngType) {
		StringBuilder builder = new StringBuilder("/explorer/");
		builder.append(legType);
		builder.append("/instances?words=");
		try {
			builder.append(URLEncoder.encode(ngram.replace(",", "\\,"), "UTF-8"));
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
		if (ngType.equalsIgnoreCase(Ngrams.Type.Case_Sensitive.name()))
			builder.append("&case=true");
		else if (ngType.equalsIgnoreCase(Ngrams.Type.Lemmas.name()))
			builder.append("&stem=true");
		return builder.toString();
	}

}
