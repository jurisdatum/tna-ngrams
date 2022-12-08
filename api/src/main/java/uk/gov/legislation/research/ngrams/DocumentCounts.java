package uk.gov.legislation.research.ngrams;

import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;

public class DocumentCounts {

	private static LinkedHashMap<String, Integer> empty(Set<String> ngrams) {
		LinkedHashMap<String, Integer> empty = new LinkedHashMap<>();
		for (String ngram : ngrams)
			empty.put(ngram, 0);
		return empty;
	}

	public static Map<String, Map<String, Integer>> invert(Map<String, Map<String, Integer>> counts) {
		Map<String, Map<String, Integer>> inverted = new HashMap<>();
		for (Entry<String, Map<String, Integer>> entry1 : counts.entrySet()) {
			String ngram = entry1.getKey();
			for (Entry<String, Integer> entry2 : entry1.getValue().entrySet()) {
				String docId = entry2.getKey();
				if (!inverted.containsKey(docId))
					inverted.put(docId, empty(counts.keySet()));
				Integer count = entry2.getValue();
				inverted.get(docId).put(ngram, count);
			}
		}
		return inverted;
	}

	private static Comparator<Entry<String, Map<String, Integer>>> comparator = new Comparator<Entry<String, Map<String, Integer>>>() {
		@Override public int compare(Entry<String, Map<String, Integer>> e1, Entry<String, Map<String, Integer>> e2) {
			int sum1 = e1.getValue().values().stream().mapToInt(Integer::intValue).sum();
			int sum2 = e2.getValue().values().stream().mapToInt(Integer::intValue).sum();
			return Integer.compare(sum2, sum1);
		}
	};
	public static LinkedHashMap<String, Map<String, Integer>> sort(Map<String, Map<String, Integer>> counts) {
		return counts.entrySet().stream()
		.sorted(comparator)
		.collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue(), (m1, m2) -> m1, LinkedHashMap::new));
	}

}
