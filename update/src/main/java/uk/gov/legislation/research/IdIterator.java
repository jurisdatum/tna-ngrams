package uk.gov.legislation.research;

import java.io.IOException;
import java.util.Calendar;
import java.util.Iterator;

import uk.gov.legislation.research.Legislation.Type;

import net.sf.saxon.s9api.XdmItem;

public class IdIterator implements Iterator<String> {
	
	private final Type type;
	private final int startYear;
	
	private int year;
	private int page;
	private Atom atom;
	private Iterator<XdmItem> ids;
	
	private void fetchNextPage() {
		page += 1;
		atom = null;
		int attempt = 0;
		while (atom == null) {
			attempt += 1;
			try {
				atom = Atom.get(type, year, page);
			} catch (IOException e) {
				if (attempt == 20) {
					System.err.println("Couldn't find Atom feed for type:" + type.name() + ", year:" + year + ", page:" + page);
					throw new RuntimeException(e);
				}
			}
		}
		ids = atom.getIds().iterator();
	}
	
	public IdIterator(Type type, int startYear) {
		this.startYear = startYear;
		this.type = type;
		this.year = Calendar.getInstance().get(Calendar.YEAR);
		this.page = 0;
		fetchNextPage();
	}

	@Override
	public boolean hasNext() {
		if (ids.hasNext())
			return true;
		if (atom.hasNextPage()) {
			fetchNextPage();
			return true;
		}
		while (year > startYear) {
			year -= 1;
			page = 0;
			fetchNextPage();
			if (ids.hasNext()) return true;
		}
		return false;
	}

	@Override
	public String next() {
		return ids.next().getStringValue().substring(33);
	}

}
