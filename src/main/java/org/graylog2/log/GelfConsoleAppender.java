package org.graylog2.log;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Layout;
import org.apache.log4j.spi.ErrorCode;
import org.apache.log4j.spi.LoggingEvent;
import org.graylog2.host.HostConfiguration;
import org.graylog2.message.GelfMessage;
import org.graylog2.message.GelfMessageBuilderException;
import org.json.simple.JSONValue;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class GelfConsoleAppender extends ConsoleAppender implements GelfMessageProvider {
	private HostConfiguration hostConfiguration;
	private GelfMessageFactory messageFactory;
	private boolean extractStacktrace;
	private boolean addExtendedInformation;
	private boolean includeLocation = true;
	private Map<String, String> fields;
	private boolean onlyShortMessage = false;

	public GelfConsoleAppender() {
		super();
		this.hostConfiguration = new HostConfiguration();
		this.messageFactory = new GelfMessageFactory();
	}

	public GelfConsoleAppender(Layout layout) {
		super(layout);
		hostConfiguration = new HostConfiguration();
	}

	public GelfConsoleAppender(Layout layout, String target) {
		super(layout, target);
		hostConfiguration = new HostConfiguration();
	}

	public void setAdditionalFields(String additionalFields) {
		fields = (Map<String, String>) JSONValue.parse(additionalFields.replaceAll("'", "\""));
	}

	public boolean isExtractStacktrace() {
		return extractStacktrace;
	}

	public void setExtractStacktrace(boolean extractStacktrace) {
		this.extractStacktrace = extractStacktrace;
	}

	public boolean isAddExtendedInformation() {
		return addExtendedInformation;
	}

	public void setAddExtendedInformation(boolean addExtendedInformation) {
		this.addExtendedInformation = addExtendedInformation;
	}

	public boolean isIncludeLocation() {
		return this.includeLocation;
	}

	public boolean isOnlyShortMessage() {
		return onlyShortMessage;
	}

	public void setOnlyShortMessage(boolean onlyShortMessage) {
		this.onlyShortMessage = onlyShortMessage;
	}

	public void setIncludeLocation(boolean includeLocation) {
		this.includeLocation = includeLocation;
	}

	public String getOriginHost() {
		return hostConfiguration.getOriginHost();
	}

	public String getFacility() {
		return hostConfiguration.getFacility();
	}

	public HostConfiguration getHostConfiguration() {
		return hostConfiguration;
	}

	public Map<String, String> getFields() {
		if (fields == null) {
			fields = new HashMap<String, String>();
		}
		return Collections.unmodifiableMap(fields);
	}

	@Override
	protected void subAppend(LoggingEvent event) {
		try {
			GelfMessage gelf = messageFactory.makeMessage(layout, event, this);
			this.qw.write(gelf.toJson());
			this.qw.write(Layout.LINE_SEP);
			if (this.immediateFlush) {
				this.qw.flush();
			}
		} catch (GelfMessageBuilderException exception) {
			errorHandler.error("Error building GELF message", exception, ErrorCode.WRITE_FAILURE);
		}
	}
}
