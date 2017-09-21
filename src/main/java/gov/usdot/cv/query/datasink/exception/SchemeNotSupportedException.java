package gov.usdot.cv.query.datasink.exception;

public class SchemeNotSupportedException extends Exception {

	private static final long serialVersionUID = 1689543198224365312L;

	public SchemeNotSupportedException(String message) {
		super(message);
	}
	
	public SchemeNotSupportedException(String message, Throwable cause) {
		super(message, cause);
	}
}