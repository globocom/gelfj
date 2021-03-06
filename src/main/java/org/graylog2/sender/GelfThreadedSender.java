package org.graylog2.sender;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.graylog2.message.GelfMessage;

public class GelfThreadedSender implements GelfSender {
	enum Status {
		ACTIVE, CLOSE_WAITING, CLOSE_FORCED, CLOSED
	}

	private static final GelfMessage CLOSE_MESSAGE = new GelfMessage();
	private static final int SHUTDOWN_TIMEOUT = 10000;
	private final GelfSender sender;
	private final BlockingQueue<GelfMessage> messageQueue;
	private final int timeout;
	private final int maxRetries;
	private Thread thread;
	private volatile Status status;

	public GelfThreadedSender(GelfSender sender, GelfSenderConfiguration configuration) {
		this.status = Status.ACTIVE;
		this.sender = sender;
		this.timeout = configuration.getThreadedQueueTimeout();
		this.maxRetries = configuration.getMaxRetries();
		this.messageQueue = new ArrayBlockingQueue<GelfMessage>(configuration.getThreadedQueueMaxDepth(), true);
	}

	public void sendMessage(GelfMessage message) throws GelfSenderException {
		if (isClosed()) {
			throw new GelfSenderException(GelfSenderException.ERROR_CODE_SHUTTING_DOWN);
		}
		if (!isInitialized()) {
			initialize();
		}
		try {
			if (!messageQueue.offer(message, timeout, TimeUnit.MILLISECONDS)) {
				throw new InterruptedException("GelfThreadedSender queue is full, discarding message");
			}
		} catch (Exception exception) {
			throw new GelfSenderException(GelfSenderException.ERROR_CODE_GENERIC_ERROR, exception);
		}
	}

	private void initialize() {
		thread = new Thread(new GelfSenderThread(), "GelfSender");
		thread.start();
	}

	private boolean isInitialized() {
		return thread != null;
	}

	public boolean isClosed() {
		return status != Status.ACTIVE;
	}

	public Status getStatus() {
		return status;
	}

	public void close() {
		if (!isClosed()) {
			if (isInitialized()) {
				status = Status.CLOSE_WAITING;
				try {
					messageQueue.offer(CLOSE_MESSAGE);
					thread.join(SHUTDOWN_TIMEOUT);
				} catch (InterruptedException ignoredException) {
				}
				if (thread.isAlive()) {
					status = Status.CLOSE_FORCED;
					thread.interrupt();
				} else {
					status = Status.CLOSED;
				}
			} else {
				status = Status.CLOSED;
			}
		}
	}

	public class GelfSenderThread implements Runnable {
		private GelfMessage currentMessage;

		public void run() {
			int retryCount = 0;
			while (isActive()) {
				if (currentMessage == null) {
					retryCount = 0;
					try {
						currentMessage = messageQueue.poll(1000, TimeUnit.MILLISECONDS);
					} catch (InterruptedException ignoredException) {
					}
				}
				if (currentMessage == CLOSE_MESSAGE) {
					currentMessage = null;
				} else if (currentMessage != null) {
					try {
						sender.sendMessage(currentMessage);
						currentMessage = null;
					} catch (GelfSenderException exception) {
						if (exception.getErrorCode() != GelfSenderException.ERROR_CODE_GENERIC_ERROR) {
							currentMessage = null;
						}
					} catch (Exception exception) {
					}
					if (currentMessage != null && retryCount++ >= maxRetries) {
						currentMessage = null;
					}
				}
			}
			sender.close();
		}

		private boolean isActive() {
			switch (getStatus()) {
			case CLOSED:
				return false;
			case CLOSE_FORCED:
				return false;
			case CLOSE_WAITING:
				return currentMessage != null || !messageQueue.isEmpty();
			default:
				return true;
			}
		}
	}
}
