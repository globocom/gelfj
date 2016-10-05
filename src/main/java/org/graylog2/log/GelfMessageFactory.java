package org.graylog2.log;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Method;

import org.apache.log4j.Layout;
import org.apache.log4j.Level;
import org.apache.log4j.spi.LocationInfo;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.log4j.spi.ThrowableInformation;
import org.graylog2.message.GelfMessage;
import org.graylog2.message.GelfMessageBuilder;
import org.graylog2.message.GelfMessageBuilderException;

public class GelfMessageFactory {
	private static Method methodGetTimeStamp = null;
	private static final String LOGGER_NDC = "loggerNdc";
	private static final String JAVA_TIMESTAMP = "timestampMs";

	static {
		Method[] declaredMethods = LoggingEvent.class.getDeclaredMethods();
		for (Method m : declaredMethods) {
			if (m.getName().equals("getTimeStamp")) {
				methodGetTimeStamp = m;
				break;
			}
		}
	}

	@SuppressWarnings("unchecked")
	public GelfMessage makeMessage(Layout layout, LoggingEvent event, GelfMessageProvider provider)
			throws GelfMessageBuilderException {
		long timeStamp = getTimeStamp(event);
		Level level = event.getLevel();

		GelfMessageBuilder builder = new GelfMessageBuilder(provider.getHostConfiguration());

		if (provider.isIncludeLocation() && event.locationInformationExists()) {
			LocationInfo locationInformation = event.getLocationInformation();
			builder.setFile(locationInformation.getFileName());
			builder.setLine(locationInformation.getLineNumber());
			builder.addField(GelfMessageBuilder.SOURCE_CLASS_FIELD, locationInformation.getClassName());
			builder.addField(GelfMessageBuilder.SOURCE_METHOD_FIELD, locationInformation.getMethodName());
		}

		String fullMessage = formatMessage(layout, event, provider);
		builder.setLevel(String.valueOf(level.getSyslogEquivalent()));
		builder.setJavaTimestamp(timeStamp);


		if (provider.isOnlyShortMessage()) {
			builder.setShortMessage(fullMessage);
		} else {
			builder.setFullMessage(fullMessage);
		}

		builder.addFields(provider.getFields());

		if (provider.isAddExtendedInformation()) {
			builder.addField(GelfMessageBuilder.THREAD_NAME_FIELD, event.getThreadName());
			builder.addField(GelfMessageBuilder.LOGGER_LEVEL_FIELD, level.toString());
			builder.addField(GelfMessageBuilder.LOGGER_NAME_FIELD, event.getLoggerName());
			builder.addField(JAVA_TIMESTAMP, timeStamp);
			builder.addFields(event.getProperties());
			String ndc = event.getNDC();
			if (ndc != null) {
				builder.addField(LOGGER_NDC, event.getNDC());
			}
		}
		return builder.build();
	}

	private String formatMessage(Layout layout, LoggingEvent event, GelfMessageProvider provider) {
		String renderedMessage = layout != null ? layout.format(event) : event.getRenderedMessage();
		if (renderedMessage == null) {
			renderedMessage = "";
		}
		if (provider.isExtractStacktrace()) {
			ThrowableInformation throwableInformation = event.getThrowableInformation();
			if (throwableInformation != null) {
				String stackTrace = extractStacktrace(throwableInformation);
				if (stackTrace != null) {
					renderedMessage += "\n\r" + extractStacktrace(throwableInformation);
				}
			}
		}
		return renderedMessage;
	}

	private String extractStacktrace(ThrowableInformation throwableInformation) {
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		Throwable t = throwableInformation.getThrowable();
		if (t != null) {
			t.printStackTrace(pw);
			return sw.toString();
		} else {
			return null;
		}
	}

	private long getTimeStamp(LoggingEvent event) {
		long timeStamp = 0;
		if (methodGetTimeStamp != null) {
			try {
				timeStamp = (Long) methodGetTimeStamp.invoke(event);
			} catch (Exception ignoredException) {
			}
		}
		return timeStamp == 0 ? System.currentTimeMillis() : timeStamp;
	}
}
