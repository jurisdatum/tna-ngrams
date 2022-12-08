package uk.gov.legislation.research.ngrams;

import java.util.*;

public class Search {
	
	public static Set<String> components(String ngram) {
		Set<String> components = new HashSet<>();
		String[] words = ngram.split(" ");
		for (int n = 1; n < words.length; n++) {
			for (int i = 0; i <= words.length - n; i++) {
				String component = String.join(" ", Arrays.copyOfRange(words, i, i + n));
				components.add(component);
			}
		}
		return components;
	}

}
