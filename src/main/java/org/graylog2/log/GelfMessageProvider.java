package org.graylog2.log;

import java.util.Map;

import org.graylog2.host.HostConfiguration;

public interface GelfMessageProvider {
	public HostConfiguration getHostConfiguration();

	public Map<String, String> getFields();
	
	public boolean isExtractStacktrace();

	public boolean isAddExtendedInformation();

	public boolean isIncludeLocation();

	boolean isOnlyShortMessage();
}