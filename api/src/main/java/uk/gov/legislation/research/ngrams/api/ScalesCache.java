package uk.gov.legislation.research.ngrams.api;

import uk.gov.legislation.research.Legislation;
import uk.gov.legislation.research.ngrams.Ngrams;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

public class ScalesCache {
	
	private static final ArrayList<Map<Short, Double>>[][] typeScales = (ArrayList<Map<Short, Double>>[][]) new ArrayList[Legislation.Type.values().length][Ngrams.Type.values().length];

	private static final ArrayList<Map<Integer, Double>>[][] groupScales = (ArrayList<Map<Integer, Double>>[][]) new ArrayList[Legislation.Group.values().length][Ngrams.Type.values().length];

	static ArrayList<Map<Short, Double>> get(Legislation.Type legType, Ngrams.Type ngType) throws IOException {
		Configuration conf = HBase.config();
		if (typeScales[legType.ordinal()][ngType.ordinal()] == null)
			typeScales[legType.ordinal()][ngType.ordinal()] = uk.gov.legislation.research.ngrams.hadoop.hbase.Scales.get(conf, legType, ngType);
		return typeScales[legType.ordinal()][ngType.ordinal()];
	}

	static ArrayList<Map<Integer, Double>> get(Legislation.Group legType, Ngrams.Type ngType) {
//		if (groupScales[legType.ordinal()][ngType.ordinal()] == null)
//			groupScales[legType.ordinal()][ngType.ordinal()] = Storage.getScales2(legType, ngType);
		return groupScales[legType.ordinal()][ngType.ordinal()];
	}
	
//	static ArrayList<Map<Integer, Double>> get(Legislation.Searchable legType, Ngrams.Type ngType) {
//		if (legType instanceof Legislation.Type)
//			return get((Legislation.Type) legType, ngType);
//		if (legType instanceof Legislation.Group)
//			return get((Legislation.Group) legType, ngType);
//		throw new IllegalArgumentException();
//	}
	
}
