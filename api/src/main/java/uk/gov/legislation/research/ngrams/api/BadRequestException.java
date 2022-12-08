package uk.gov.legislation.research.ngrams.api;

import javax.servlet.ServletException;

public class BadRequestException extends ServletException {

	public BadRequestException(String message) {
		super(message);
	}

}
