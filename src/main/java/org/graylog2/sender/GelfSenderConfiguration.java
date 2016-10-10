package org.graylog2.sender;

public class GelfSenderConfiguration {
	private static final String DEFAULT_PROTOCOL = "udp";
	public static final int DEFAULT_PORT = 12201;

	private String protocol;
	private String graylogURI;
	private String graylogHost;
	private int graylogPort;
	private String amqpURI;
	private String amqpExchangeName;
	private String amqpRoutingKey;
	private boolean tcpKeepalive;
	private boolean threaded;
	private int threadedQueueMaxDepth;
	private int threadedQueueTimeout;
	private int socketSendBufferSize;
	private int sendTimeout;
	private int maxRetries;
	private boolean blocking = false;
	private boolean debug = false;

	public GelfSenderConfiguration() {
		this.protocol = DEFAULT_PROTOCOL;
		this.graylogPort = DEFAULT_PORT;
		this.maxRetries = 5;
		this.sendTimeout = 1000;
		this.threadedQueueTimeout = 1000;
		this.threadedQueueMaxDepth = 1000;
	}

	public String getGraylogURI() {
		if (graylogURI == null) {
			return protocol + ":" + graylogHost + ":" + graylogPort;
		}
		return graylogURI;
	}

	public void setGraylogURI(String graylogURI) {
		this.graylogURI = graylogURI;
		if (graylogURI != null) {
			String[] parts = graylogURI.split(":");
			if (parts.length == 0 || parts.length > 3) {
				throw new IllegalArgumentException("Unsupported URI format: " + graylogURI);
			}
			if (parts.length == 1) {
				setGraylogHost(parts[0]);
			} else {
				setProtocol(parts[0]);
				if (!"http".equals(getProtocol()) && !"https".equals(getProtocol())) {
					setGraylogHost(parts[1]);
					if (parts.length == 3) {
						setGraylogPort(Integer.valueOf(parts[2]));
					}
				}
			}
		}
	}

	public String getProtocol() {
		return protocol;
	}

	public void setProtocol(String protocol) {
		this.protocol = protocol != null ? protocol.toLowerCase() : null;
	}

	public String getGraylogHost() {
		return graylogHost;
	}

	public void setGraylogHost(String graylogHost) {
		this.graylogHost = graylogHost;
	}

	public int getGraylogPort() {
		return graylogPort;
	}

	public void setGraylogPort(int graylogPort) {
		this.graylogPort = graylogPort;
	}

	public String getAmqpURI() {
		return amqpURI;
	}

	public void setAmqpURI(String amqpURI) {
		if (amqpURI != null) {
			setProtocol("amqp");
			this.amqpURI = amqpURI;
		}
	}

	public String getAmqpExchangeName() {
		return amqpExchangeName;
	}

	public void setAmqpExchangeName(String amqpExchangeName) {
		this.amqpExchangeName = amqpExchangeName;
	}

	public String getAmqpRoutingKey() {
		return amqpRoutingKey;
	}

	public void setAmqpRoutingKey(String amqpRoutingKey) {
		this.amqpRoutingKey = amqpRoutingKey;
	}

	public int getSocketSendBufferSize() {
		return socketSendBufferSize;
	}

	public void setSocketSendBufferSize(int socketSendBufferSize) {
		this.socketSendBufferSize = socketSendBufferSize;
	}

	public boolean isTcpKeepalive() {
		return tcpKeepalive;
	}

	public void setTcpKeepalive(boolean tcpKeepalive) {
		this.tcpKeepalive = tcpKeepalive;
	}

	public boolean isThreaded() {
		return threaded;
	}

	public void setThreaded(boolean threaded) {
		this.threaded = threaded;
	}

	public int getThreadedQueueMaxDepth() {
		return threadedQueueMaxDepth;
	}

	public void setThreadedQueueMaxDepth(int threadedQueueMaxDepth) {
		this.threadedQueueMaxDepth = threadedQueueMaxDepth;
	}
	
	public int getThreadedQueueTimeout() {
		return threadedQueueTimeout;
	}
	
	public void setThreadedQueueTimeout(int threadedQueueTimeout) {
		this.threadedQueueTimeout = threadedQueueTimeout;
	}
	
	public int getSendTimeout() {
		return sendTimeout;
	}
	
	public void setSendTimeout(int sendTimeout) {
		this.sendTimeout = sendTimeout;
	}
	
	public int getMaxRetries() {
		return maxRetries;
	}

	public void setMaxRetries(int maxRetries) {
		this.maxRetries = maxRetries;
	}

	public boolean isBlocking() {
		return blocking;
	}

	public void setBlocking(boolean blocking) {
		this.blocking = blocking;
	}

	public boolean isDebug() {
		return debug;
	}

	public void setDebug(boolean debug) {
		this.debug = debug;
	}
}