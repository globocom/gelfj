package org.graylog2.log;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.net.SocketException;
import java.net.UnknownHostException;

import org.apache.log4j.Category;
import org.apache.log4j.Level;
import org.apache.log4j.MDC;
import org.apache.log4j.NDC;
import org.apache.log4j.spi.LoggingEvent;
import org.graylog2.message.GelfMessage;
import org.graylog2.sender.GelfSender;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Anton Yakimov
 * @author Jochen Schalanda
 */
public class GelfAppenderTest {
	private static final String CLASS_NAME = GelfAppenderTest.class.getCanonicalName();
	private TestGelfSender gelfSender;
	private GelfAppender gelfAppender;

	@Before
	public void setUp() throws IOException {
		gelfSender = new TestGelfSender();

		gelfAppender = new GelfAppender() {

			@Override
			public GelfSender getGelfSender() {
				return gelfSender;
			}

			@Override
			public void append(LoggingEvent event) {
				super.append(event);
			}
		};
	}

	@After
	public void tearDown() {
		if (gelfAppender.isAddExtendedInformation()) {
			NDC.clear();
		}
	}

	@Test
	public void ensureHostnameForMessage() {

		LoggingEvent event = new LoggingEvent(CLASS_NAME, Category.getInstance(GelfAppenderTest.class), 123L,
				Level.INFO, "Das Auto", new RuntimeException("Volkswagen"));
		gelfAppender.append(event);

		assertThat("Message hostname", gelfSender.getLastMessage().getHost(), notNullValue());

		gelfAppender.setOriginHost("example.com");
		gelfAppender.append(event);
		assertThat(gelfSender.getLastMessage().getHost(), is("example.com"));
	}

	@Test
	public void handleNullInAppend() {

		LoggingEvent event = new LoggingEvent(CLASS_NAME, Category.getInstance(this.getClass()), 123L, Level.INFO, null,
				new RuntimeException("LOL"));
		gelfAppender.append(event);

		assertThat("Message short message", gelfSender.getLastMessage().getShortMessage(), notNullValue());
		assertThat("Message full message", gelfSender.getLastMessage().getFullMessage(), notNullValue());
	}

	@Test
	public void handleMDC() {

		gelfAppender.setAddExtendedInformation(true);

		LoggingEvent event = new LoggingEvent(CLASS_NAME, Category.getInstance(this.getClass()), 123L, Level.INFO, "",
				new RuntimeException("LOL"));
		MDC.put("foo", "bar");

		gelfAppender.append(event);

		assertEquals("bar", gelfSender.getLastMessage().getAdditionalFields().get("foo"));
		assertNull(gelfSender.getLastMessage().getAdditionalFields().get("non-existent"));
	}

	@Test
	public void handleMDCTransform() {

		gelfAppender.setAddExtendedInformation(true);

		LoggingEvent event = new LoggingEvent(CLASS_NAME, Category.getInstance(this.getClass()), 123L, Level.INFO, "",
				new RuntimeException("LOL"));
		MDC.put("foo", 200);

		gelfAppender.append(event);

		assertEquals(200, gelfSender.getLastMessage().getAdditionalFields().get("foo"));
		assertNull(gelfSender.getLastMessage().getAdditionalFields().get("non-existent"));

		event = new LoggingEvent(CLASS_NAME, Category.getInstance(this.getClass()), 123L, Level.INFO, "",
				new RuntimeException("LOL"));
		gelfAppender.append(event);

		assertEquals(new Integer(200), gelfSender.getLastMessage().getAdditionalFields().get("foo"));
		assertNull(gelfSender.getLastMessage().getAdditionalFields().get("non-existent"));
	}

	@Test
	public void handleNDC() {
		gelfAppender.setAddExtendedInformation(true);

		LoggingEvent event = new LoggingEvent(CLASS_NAME, Category.getInstance(this.getClass()), 123L, Level.INFO, "",
				new RuntimeException("LOL"));
		NDC.push("Foobar");

		gelfAppender.append(event);

		assertEquals("Foobar", gelfSender.getLastMessage().getAdditionalFields().get("context"));
	}

	@Test
	public void disableExtendedInformation() {

		gelfAppender.setAddExtendedInformation(false);

		LoggingEvent event = new LoggingEvent(CLASS_NAME, Category.getInstance(this.getClass()), 123L, Level.INFO, "",
				new RuntimeException("LOL"));

		MDC.put("foo", "bar");
		NDC.push("Foobar");

		gelfAppender.append(event);

		assertNull(gelfSender.getLastMessage().getAdditionalFields().get("loggerNdc"));
		assertNull(gelfSender.getLastMessage().getAdditionalFields().get("foo"));
	}

	@Test
	public void checkExtendedInformation() throws UnknownHostException, SocketException {

		gelfAppender.setAddExtendedInformation(true);

		LoggingEvent event = new LoggingEvent(CLASS_NAME, Category.getInstance(GelfAppenderTest.class), 123L,
				Level.INFO, "Das Auto", new RuntimeException("LOL"));

		gelfAppender.append(event);

		assertEquals(gelfSender.getLastMessage().getAdditionalFields().get("javaclass"), CLASS_NAME);
	}

	private class TestGelfSender implements GelfSender {
		private GelfMessage lastMessage;

		public void sendMessage(GelfMessage message) {
			this.lastMessage = message;
		}

		public void close() {
		}

		public GelfMessage getLastMessage() {
			return lastMessage;
		}
	}
}
