package org.graylog2.log;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.ErrorManager;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.ErrorCode;
import org.apache.log4j.spi.LoggingEvent;
import org.graylog2.host.HostConfiguration;
import org.graylog2.message.GelfMessage;
import org.graylog2.message.GelfMessageBuilderException;
import org.graylog2.sender.GelfSender;
import org.graylog2.sender.GelfSenderConfiguration;
import org.graylog2.sender.GelfSenderConfigurationException;
import org.graylog2.sender.GelfSenderException;
import org.graylog2.sender.GelfSenderFactory;
import org.json.simple.JSONValue;

/**
 *
 * @author Anton Yakimov
 * @author Jochen Schalanda
 */
public class GelfAppender extends AppenderSkeleton implements GelfMessageProvider {
	private HostConfiguration hostConfiguration;
	private GelfSenderConfiguration senderConfiguration;
	private GelfSender gelfSender;
	private GelfMessageFactory messageFactory;
	private boolean extractStacktrace;
	private boolean onlyShortMessage = false;
	private boolean addExtendedInformation;
	private boolean includeLocation = true;
	private Map<String, String> fields;

	public GelfAppender() {
		super();
		this.hostConfiguration = new HostConfiguration();
		this.senderConfiguration = new GelfSenderConfiguration();
		this.messageFactory = new GelfMessageFactory();
	}

	@SuppressWarnings("unchecked")
	public void setAdditionalFields(String additionalFields) {
		fields = (Map<String, String>) JSONValue.parse(additionalFields.replaceAll("'", "\""));
	}

	public String getGraylogHost() {
		return senderConfiguration.getGraylogURI();
	}

	public void setGraylogHost(String graylogHost) {
		senderConfiguration.setGraylogURI(graylogHost);
	}

	public int getGraylogPort() {
		return senderConfiguration.getGraylogPort();
	}

	public boolean isTcpKeepalive() {
		return senderConfiguration.isTcpKeepalive();
	}

	public void setTcpKeepalive(boolean tcpKeepalive) {
		senderConfiguration.setTcpKeepalive(tcpKeepalive);
	}

	public void setGraylogPort(int graylogPort) {
		senderConfiguration.setGraylogPort(graylogPort);
	}

	public int getSocketSendBufferSize() {
		return senderConfiguration.getSocketSendBufferSize();
	}

	public void setSocketSendBufferSize(int socketSendBufferSize) {
		senderConfiguration.setSocketSendBufferSize(socketSendBufferSize);
	}

	public boolean isThreaded() {
		return senderConfiguration.isThreaded();
	}

	public void setThreaded(boolean threaded) {
		senderConfiguration.setThreaded(threaded);
	}

	public int getThreadedQueueMaxDepth() {
		return senderConfiguration.getThreadedQueueMaxDepth();
	}

	public void setThreadedQueueMaxDepth(int threadedQueueMaxDepth) {
		senderConfiguration.setThreadedQueueMaxDepth(threadedQueueMaxDepth);
	}

	public int getThreadedQueueTimeout() {
		return senderConfiguration.getThreadedQueueTimeout();
	}

	public void setThreadedQueueTimeout(int threadedQueueTimeout) {
		senderConfiguration.setThreadedQueueTimeout(threadedQueueTimeout);
	}

	public String getAmqpURI() {
		return senderConfiguration.getAmqpURI();
	}

	public void setAmqpURI(String amqpURI) {
		senderConfiguration.setAmqpURI(amqpURI);
	}

	public String getAmqpExchangeName() {
		return senderConfiguration.getAmqpExchangeName();
	}

	public void setAmqpExchangeName(String amqpExchangeName) {
		senderConfiguration.setAmqpExchangeName(amqpExchangeName);
	}

	public String getAmqpRoutingKey() {
		return senderConfiguration.getAmqpRoutingKey();
	}

	public void setAmqpRoutingKey(String amqpRoutingKey) {
		senderConfiguration.setAmqpRoutingKey(amqpRoutingKey);
	}

	public int getSendTimeout() {
		return senderConfiguration.getSendTimeout();
	}

	public void setSendTimeout(int sendTimeout) {
		senderConfiguration.setSendTimeout(sendTimeout);
	}

	public int getMaxRetries() {
		return senderConfiguration.getMaxRetries();
	}

	public void setMaxRetries(int amqpMaxRetries) {
		senderConfiguration.setMaxRetries(amqpMaxRetries);
	}

	@Deprecated
	public int getAmqpMaxRetries() {
		return senderConfiguration.getMaxRetries();
	}

	@Deprecated
	public void setAmqpMaxRetries(int amqpMaxRetries) {
		senderConfiguration.setMaxRetries(amqpMaxRetries);
	}

	public boolean isExtractStacktrace() {
		return extractStacktrace;
	}

	public void setExtractStacktrace(boolean extractStacktrace) {
		this.extractStacktrace = extractStacktrace;
	}

	public String getOriginHost() {
		return hostConfiguration.getOriginHost();
	}

	public void setOriginHost(String originHost) {
		hostConfiguration.setOriginHost(originHost);
	}

	public HostConfiguration getHostConfiguration() {
		return hostConfiguration;
	}

	public String getFacility() {
		return hostConfiguration.getFacility();
	}

	public void setFacility(String facility) {
		hostConfiguration.setFacility(facility);
	}

	public boolean isAddExtendedInformation() {
		return addExtendedInformation;
	}

	public boolean isOnlyShortMessage() {
		return onlyShortMessage;
	}

	public void setOnlyShortMessage(boolean onlyShortMessage) {
		this.onlyShortMessage = onlyShortMessage;
	}


	public void setAddExtendedInformation(boolean addExtendedInformation) {
		this.addExtendedInformation = addExtendedInformation;
	}

	public boolean isIncludeLocation() {
		return this.includeLocation;
	}

	public void setIncludeLocation(boolean includeLocation) {
		this.includeLocation = includeLocation;
	}

	public Map<String, String> getFields() {
		if (fields == null) {
			fields = new HashMap<String, String>();
		}
		return Collections.unmodifiableMap(fields);
	}

	@Override
	public void activateOptions() {
		try {
			if (gelfSender != null) {
				gelfSender.close();
			}
			gelfSender = GelfSenderFactory.getInstance().createSender(senderConfiguration);
		} catch (GelfSenderConfigurationException exception) {
			errorHandler.error(exception.getMessage(), exception.getCauseException(), ErrorManager.WRITE_FAILURE);
		} catch (Exception exception) {
			errorHandler.error("Could not activate GELF appender", exception, ErrorManager.WRITE_FAILURE);
		}
	}

	@Override
	protected void append(LoggingEvent event) {
		GelfSender sender = getGelfSender();
		if (sender == null) {
			errorHandler.error("Could not send GELF message. Gelf Sender is not initialised and equals null");
		} else {
			try {
				GelfMessage gelfMessage = messageFactory.makeMessage(layout, event, this);
				sender.sendMessage(gelfMessage);
			} catch (GelfMessageBuilderException exception) {
				errorHandler.error("Error building GELF message", exception, ErrorCode.WRITE_FAILURE);
			} catch (GelfSenderException exception) {
				errorHandler.error("Error during sending GELF message. Error code: " + exception.getErrorCode() + ".",
						exception.getCause(), ErrorCode.WRITE_FAILURE);
			}
		}
	}

	public GelfSender getGelfSender() {
		return gelfSender;
	}

	public GelfMessageFactory getMessageFactory() {
		return messageFactory;
	}

	public void close() {
		GelfSender x = this.getGelfSender();
		if (x != null) {
			x.close();
		}
	}

	public boolean requiresLayout() {
		return true;
	}
}
