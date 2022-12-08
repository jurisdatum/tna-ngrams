package uk.gov.legislation.research.ngrams;

import java.util.Map;
import java.util.Map.Entry;

public class Aggregate {
	
	private static void miniMerge(Map<String, Integer> totals, Map<String, Integer> additions) {
		for (Entry<String, Integer> addition : additions.entrySet()) {
			String key = addition.getKey();
			if (totals.containsKey(key))
				totals.put(key, totals.get(key) + addition.getValue());
			else
				totals.put(key, addition.getValue());				
		}
	}
	private static void merge(Map<String, Map<String, Integer>> totals, Map<String, Map<String, Integer>> additions) {
		for (Entry<String, Map<String, Integer>> addition : additions.entrySet()) {
			String ngram = addition.getKey();
			if (totals.containsKey(ngram))
				miniMerge(totals.get(ngram), addition.getValue());
			else
				totals.put(ngram, addition.getValue());
		}
	}

}
