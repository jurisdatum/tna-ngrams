package uk.gov.legislation.research.ngrams.api;

import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.BiConsumer;
import java.util.function.IntConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

class DateRange {
	
	final int start, end;
	
	DateRange(int start, int end) {
		if (start > end) throw new IllegalArgumentException();
		this.start = start;
		this.end = end;
	}
	
	int count() {
		return end - start + 1;
	}
	
	SortedSet<Integer> getYears()  {
		return IntStream.rangeClosed(start, end).mapToObj(Integer::new).collect(Collectors.toCollection(TreeSet::new));
	}
	
	void forEach(IntConsumer callback) {
		IntStream.rangeClosed(start, end).forEach(callback);;
	}
	
	@Override
	public String toString() {
		if (start == end)
			return Integer.toString(start);
		return start + "-" + end;
	}
	
	static class MonthRange extends DateRange {
		
		final int startMonth, endMonth;
		
		MonthRange(int startYear, int startMonth, int endYear, int endMonth) {
			super(startYear, endYear);
			if (startYear == endYear && startMonth > endMonth) throw new IllegalArgumentException();
			this.startMonth = startMonth;
			this.endMonth = endMonth;
		}
		
		void forEach(BiConsumer<Integer, Integer> f) {
			if (start == end) {
				for (int month = startMonth; month <= endMonth; month++) f.accept(start, month);
			} else {
				for (int month = startMonth; month <= 12; month++) f.accept(start, month);
				for (int year = start + 1; year < end; year++)
					for (int month = 1; month <= 12; month++) f.accept(year, month);
				for (int month = 1; month <= endMonth; month++) f.accept(end, month);
			}
		}
		
		int monthCount() {
			if (start == end)
				return (endMonth - startMonth) + 1;
			if (end - start == 1)
				return (13 - startMonth) + endMonth;
			return (13 - startMonth) + (((end - start) - 1) * 12) + endMonth;
		}

		SortedMap<Integer, SortedSet<Integer>> getMonths()  {
			TreeMap<Integer, SortedSet<Integer>> months = new TreeMap<>();
			if (start == end) {
				TreeSet<Integer> set = new TreeSet<>();
				for (int month = startMonth; month <= endMonth; month++)
					set.add(month);
				months.put(start, set);
			} else {
				TreeSet<Integer> set = new TreeSet<>();
				for (int month = startMonth; month <= 12; month++) set.add(month);
				months.put(start, set);
				for (int year = start + 1; year < end; year++) {
					set = new TreeSet<>();
					for (int month = 1; month <= 12; month++) set.add(month);
					months.put(year, set);
				}
				set = new TreeSet<>();
				for (int month = 1; month <= endMonth; month++) set.add(month);
				months.put(end, set);
			}
			return months;
		}
		
		private static final String[] monthNames = new String[] { "",
			"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec" };
		
		@Override
		public String toString() {
			if (start == end) {
				if (startMonth == endMonth) {
					return monthNames[startMonth] + " " + start;
				} else {
					return monthNames[startMonth] + "-" + monthNames[endMonth] + " " + start;
				}
			} else {
				return monthNames[startMonth] + " " + start + " - " + monthNames[endMonth] + " " + end;
			}
		}
		
	}

}
