package org.graylog2.message;

import java.util.Map;
import java.util.Map.Entry;

import org.graylog2.host.HostConfiguration;

public class GelfMessageBuilder {
    public static final String LOGGER_NAME_FIELD = "javaclass";
    public static final String THREAD_NAME_FIELD = "thread";
	public static final String LOGGER_LEVEL_FIELD = "level";
	public static final String SOURCE_CLASS_FIELD = "SourceClassName";
	public static final String SOURCE_METHOD_FIELD = "SourceMethodName";
	
	private GelfMessage gelfMessage;

	public GelfMessageBuilder(HostConfiguration hostConfiguration) {
		this.gelfMessage = new GelfMessage();
		gelfMessage.setHost(hostConfiguration.getOriginHost());
		gelfMessage.setFacility(hostConfiguration.getFacility());
	}

	public GelfMessageBuilder setFullMessage(String fullMessage) {
		gelfMessage.setFullMessage(fullMessage);
		if (gelfMessage.getShortMessage() == null && fullMessage != null) {
			setShortMessage(formatShortMessage(fullMessage));
		}
		return this;
	}

	private String formatShortMessage(String fullMessage) {
		String shortMessage = fullMessage;
		int lineBreak = fullMessage.indexOf('\n');
		if (lineBreak != -1) {
			shortMessage = fullMessage.substring(0, lineBreak).trim();
		}
		return shortMessage;
	}

	public GelfMessageBuilder setShortMessage(String shortMessage) {
		gelfMessage.setShortMessage(shortMessage);
		return this;
	}

	public GelfMessageBuilder setLevel(String level) {
		gelfMessage.setLevel(level);
		return this;
	}

	public GelfMessageBuilder setJavaTimestamp(long javaTimestamp) {
		gelfMessage.setJavaTimestamp(javaTimestamp);
		return this;
	}

	public GelfMessageBuilder setFile(String file) {
		gelfMessage.setFile(file);
		return this;
	}

	public GelfMessageBuilder setLine(String line) {
		gelfMessage.setLine(line);
		return this;
	}

	public GelfMessageBuilder addField(String key, Object value) {
		gelfMessage.addField(key, value);
		return this;
	}

	public GelfMessageBuilder addFields(Map<String, ? extends Object> fields) {
		if (fields != null) {
			for (Entry<String, ? extends Object> entry : fields.entrySet()) {
				gelfMessage.addField(entry.getKey(), entry.getValue());
			}
		}
		return this;
	}

	private void validate() throws GelfMessageBuilderException {
		if (!isShortOrFullMessagesExists()) {
			throw new GelfMessageBuilderException("Gelf message does not contain message content");
		}
		if (isEmpty(gelfMessage.getVersion())) {
			throw new GelfMessageBuilderException("Gelf message version is not set");
		}
		if (isEmpty(gelfMessage.getHost())) {
			throw new GelfMessageBuilderException("Gelf message host is not set");
		}
	}

	private boolean isShortOrFullMessagesExists() {
		return gelfMessage.getShortMessage() != null || gelfMessage.getFullMessage() != null;
	}

	public boolean isEmpty(String str) {
		return str == null || "".equals(str.trim());
	}

	public GelfMessage build() throws GelfMessageBuilderException {
		validate();
		return gelfMessage;
	}
}
