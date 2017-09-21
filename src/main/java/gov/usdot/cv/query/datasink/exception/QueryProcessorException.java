package gov.usdot.cv.query.datasink.exception;

public class QueryProcessorException extends Exception {

	private static final long serialVersionUID = -2149949638579876365L;

	public QueryProcessorException(String message) {
		super(message);
	}
	
	public QueryProcessorException(String message, Throwable cause) {
		super(message, cause);
	}
}