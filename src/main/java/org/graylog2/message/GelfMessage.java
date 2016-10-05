package org.graylog2.message;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.graylog2.json.JSON;

public class GelfMessage {
	private static final String ID_NAME = "id";
	private static final String GELF_VERSION = "1.1";
	private static final BigDecimal TIME_DIVISOR = new BigDecimal(1000);

	private String version = GELF_VERSION;
	private String host;
	private String shortMessage;
	private String fullMessage;
	private long javaTimestamp;
	private String level;
	private String facility;
	private String line;
	private String file;
	private Map<String, Object> additionalFields;

	public GelfMessage() {
		this.version = GELF_VERSION;
	}

	public String toJson() {
		StringBuilder sb = new StringBuilder();
		sb.append("{\r\n");
		sb.append("\t\"version\": ").append(JSON.encodeQuoted(getVersion())).append(",\r\n");
		sb.append("\t\"host\": ").append(JSON.encodeQuoted(getHost())).append(",\r\n");
		sb.append("\t\"short_message\": ").append(JSON.encodeQuoted(getShortMessage())).append(",\r\n");
		if ( getFullMessage() != null) {
			sb.append("\t\"full_message\": ").append(JSON.encodeQuoted(getFullMessage())).append(",\r\n");
		}
		sb.append("\t\"timestamp\": ").append(getTimestamp()).append(",\r\n");
		sb.append("\t\"facility\": ").append(JSON.encodeQuoted(getFacility())).append(",\r\n");
		if (getFile() != null) {
			sb.append("\t\"file\": ").append(JSON.encodeQuoted(getFile())).append(",\r\n");
		}
		Long line = getLine();
		if (line != null) {
			sb.append("\t\"line\": ").append(line).append(",\r\n");
		}
		sb.append("\t\"level\": ").append(getLevel()).append("\r\n");

		for (Map.Entry<String, Object> additionalField : getAdditionalFields().entrySet()) {
			if (!ID_NAME.equals(additionalField.getKey())) {
				String key = JSON.encodeQuoted("_" + additionalField.getKey());
				Object objectValue = additionalField.getValue();
				String value;
				if (objectValue != null) {
					if (objectValue instanceof Number) {
						value = objectValue.toString();
					} else if (objectValue instanceof Boolean) {
						value = objectValue.toString();
					} else {
						value = JSON.encodeQuoted(additionalField.getValue().toString());
					}
				} else {
					value = "null";
				}
				sb.append(",\r\n\t").append(key).append(": ").append(value);
			}
		}
		sb.append("}\r\n");
		return sb.toString();
	}

	public int getCurrentMillis() {
		return (int) System.currentTimeMillis();
	}

	public String getVersion() {
		return version;
	}

	public void setVersion(String version) {
		this.version = version;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public String getShortMessage() {
		return shortMessage;
	}

	public void setShortMessage(String shortMessage) {
		this.shortMessage = shortMessage;
	}

	public String getFullMessage() {
		return fullMessage;
	}

	public void setFullMessage(String fullMessage) {
		this.fullMessage = fullMessage;
	}

	public String getTimestamp() {
		return new BigDecimal(javaTimestamp).divide(TIME_DIVISOR).toPlainString();
	}

	public Long getJavaTimestamp() {
		return javaTimestamp;
	}

	public void setJavaTimestamp(long javaTimestamp) {
		this.javaTimestamp = javaTimestamp;
	}

	public long getLevel() {
		long level;
		try {
			level = Long.parseLong(this.level);
		} catch (NumberFormatException e) {
			// fallback to info
			level = 6L;
		}
		return level;
	}

	public void setLevel(String level) {
		this.level = level;
	}

	public String getFacility() {
		return facility;
	}

	public void setFacility(String facility) {
		this.facility = facility;
	}

	public Long getLine() {
		if (this.line == null) {
			return null;
		}
		try {
			return Long.parseLong(this.line);
		} catch (NumberFormatException exception) {
			return -1L;
		}
	}

	public void setLine(String line) {
		this.line = line;
	}

	public String getFile() {
		return file;
	}

	public void setFile(String file) {
		this.file = file;
	}

	public void addField(String key, Object value) {
		if (additionalFields == null) {
			additionalFields = new HashMap<String, Object>();
		}
		additionalFields.put(key, value);
	}

	public Map<String, Object> getAdditionalFields() {
		if (additionalFields == null) {
			return Collections.emptyMap();
		}
		return additionalFields;
	}
}
