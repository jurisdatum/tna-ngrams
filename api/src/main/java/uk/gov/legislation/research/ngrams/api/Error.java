package uk.gov.legislation.research.ngrams.api;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@WebServlet("/error")
public class Error extends HttpServlet {
	
	@Override
	public void doGet(HttpServletRequest request, HttpServletResponse response) {
		
		Integer statusCode = (Integer) request.getAttribute("javax.servlet.error.status_code");
		Class<? extends Throwable> exceptionType = (Class<? extends Throwable>) request.getAttribute("javax.servlet.error.exception_type");
		String message = (String) request.getAttribute("javax.servlet.error.message");
		Throwable exception = (Throwable) request.getAttribute("javax.servlet.error.exception");
		String requestUri = (String) request.getAttribute("javax.servlet.error.request_uri");

		if (exception != null && exception instanceof BadRequestException)
			response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
		else if (statusCode != null)
			response.setStatus(statusCode);
		else
			response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
		
		response.setContentType("text/plain");
		
		PrintWriter out;
		try {
			out = response.getWriter();
		} catch (IOException e) {
			return;
		}
		if (exception != null)
			out.println(exception.getLocalizedMessage());
		else if (statusCode != null && statusCode.intValue() == HttpServletResponse.SC_NOT_FOUND)
			out.println("not found");
		else
			out.println("server error");
	}

}
