package uk.gov.legislation.research.ngrams.api;

import uk.gov.legislation.research.Legislation;
import uk.gov.legislation.research.ngrams.Ngrams;
import uk.gov.legislation.research.ngrams.hadoop.hbase.Scales;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

public class ScalesCache {
	
	private static final ArrayList<Map<Short, Double>>[][] typeScales = (ArrayList<Map<Short, Double>>[][]) new ArrayList[Legislation.Type.values().length][Ngrams.Type.values().length];

	private static final ArrayList<Map<Short, Double>>[][] groupScales = (ArrayList<Map<Short, Double>>[][]) new ArrayList[Legislation.Group.values().length][Ngrams.Type.values().length];

	private static ArrayList<Map<Short, Double>> getType(Legislation.Type legType, Ngrams.Type ngType) throws IOException {
		int x = legType.ordinal();
		int y = ngType.ordinal();
		if (typeScales[x][y] == null)
			typeScales[x][y] = Scales.get(HBase.config(), legType, ngType);
		return typeScales[x][y];
	}

	private static ArrayList<Map<Short, Double>> getGroup(Legislation.Group legType, Ngrams.Type ngType) throws IOException {
		int x = legType.ordinal();
		int y = ngType.ordinal();
		if (groupScales[x][y] == null)
			groupScales[x][y] = Scales.get(HBase.config(), legType, ngType);
		return groupScales[x][y];
	}
	
	static ArrayList<Map<Short, Double>> get(Legislation.Searchable legType, Ngrams.Type ngType) throws IOException {
		if (legType instanceof Legislation.Type)
			return getType((Legislation.Type) legType, ngType);
		if (legType instanceof Legislation.Group)
			return getGroup((Legislation.Group) legType, ngType);
		throw new IllegalArgumentException();
	}
	
}
