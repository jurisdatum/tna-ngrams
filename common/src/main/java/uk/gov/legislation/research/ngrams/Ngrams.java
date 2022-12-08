package uk.gov.legislation.research.ngrams;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.TreeMap;

import edu.stanford.nlp.ling.CoreAnnotations.LemmaAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.process.PTBTokenizer;
import edu.stanford.nlp.util.CoreMap;
import edu.stanford.nlp.util.StringUtils;

public class Ngrams {
	
	public enum Type { Case_Sensitive("case sensitive"), Case_Insensitive("case insensitive"), Lemmas("word stems");
		public final String description;
		private Type(String description) { this.description = description; }
		public String toString() { return super.toString().toLowerCase(); }
	}
	
	private static String getWord(CoreLabel token, Type type) {
		switch (type) {
			case Case_Sensitive: return token.get(TextAnnotation.class);
			case Case_Insensitive: return token.get(TextAnnotation.class).toLowerCase();
			case Lemmas: return token.get(LemmaAnnotation.class);
			default: throw new IllegalArgumentException();
		}
	}
	
	private static final StanfordCoreNLP nlp;
	static {
		Properties props = new Properties();
		props.put("annotators", "tokenize, ssplit, pos, lemma");
		nlp = new StanfordCoreNLP(props);
	}
	
	public static String tokenize(String text, Type type) {
		Annotation annotation = new Annotation(text);
		nlp.annotate(annotation);
		Iterator<CoreLabel> labels = annotation.get(TokensAnnotation.class).iterator();
		StringBuilder builder = new StringBuilder();
		if (labels.hasNext())
			builder.append(getWord(labels.next(), type));
		while (labels.hasNext()) {
			builder.append(" ");
			builder.append(getWord(labels.next(), type));
		}
		return builder.toString();
	}
		
	public static String untokenize(String text) {
		return PTBTokenizer.ptb2Text(text)
			.replace("-lrb-", "(")
			.replace("-rrb-", ")")
			.replace("-LRB-", "(")
			.replace("-RRB-", ")");
	}

	private final Annotation annotation;
	
	public Ngrams(String text) {
		annotation = new Annotation(text);
		nlp.annotate(annotation);
	}
	
	public TreeMap<String, Integer> get(int n, Type type) {
		if (n < 1 || n > Globals.MAX_N)
			throw new IllegalArgumentException();
		TreeMap<String, Integer> counter = new TreeMap<>();
		for (CoreMap sentence: annotation.get(SentencesAnnotation.class)) {
			List<CoreLabel> tokens = sentence.get(TokensAnnotation.class);
			if (tokens.size() < n)
				continue;
			LinkedList<String> ngram = new LinkedList<>();
			for (CoreLabel token : tokens.subList(0, n)) {
				String word = getWord(token, type);
				ngram.add(word);
			}
			String ngramStr = StringUtils.join(ngram, " ");
			int count = counter.containsKey(ngramStr) ? counter.get(ngramStr) : 0;
			counter.put(ngramStr, count + 1);
			for (CoreLabel token : tokens.subList(n, tokens.size())) {
				ngram.pop();
				String word = getWord(token, type);
				ngram.add(word);
				ngramStr = StringUtils.join(ngram, " ");
				count = counter.containsKey(ngramStr) ? counter.get(ngramStr) : 0;
				counter.put(ngramStr, count + 1);
			}
		}
		return counter;
	}
}
