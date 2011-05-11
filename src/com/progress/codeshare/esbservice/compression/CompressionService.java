package com.progress.codeshare.esbservice.compression;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import com.sonicsw.xq.XQConstants;
import com.sonicsw.xq.XQEnvelope;
import com.sonicsw.xq.XQInitContext;
import com.sonicsw.xq.XQMessage;
import com.sonicsw.xq.XQMessageFactory;
import com.sonicsw.xq.XQParameters;
import com.sonicsw.xq.XQPart;
import com.sonicsw.xq.XQService;
import com.sonicsw.xq.XQServiceContext;
import com.sonicsw.xq.XQServiceException;
import com.sonicsw.xq.service.sj.MessageUtils;

public final class CompressionService implements XQService {
	private static final String DIRECTION_FROM = "From";

	private static final String DIRECTION_TO = "To";

	private static final String FORMAT_GZIP = "GZIP";

	private static final String FORMAT_ZIP = "Zip";

	private static final String PARAM_NAME_DIRECTION = "direction";

	private static final String PARAM_NAME_FORMAT = "format";

	private static final String PARAM_NAME_KEEP_ORIGINAL_PART = "keepOriginalPart";

	private static final String PARAM_NAME_MESSAGE_PART = "messagePart";

	public void destroy() {
	}

	public void init(XQInitContext initCtx) throws XQServiceException {
	}

	public void service(final XQServiceContext servCtx)
			throws XQServiceException {

		try {
			final XQParameters params = servCtx.getParameters();

			final String direction = params.getParameter(PARAM_NAME_DIRECTION,
					XQConstants.PARAM_STRING);

			final String format = params.getParameter(PARAM_NAME_FORMAT,
					XQConstants.PARAM_STRING);

			final XQMessageFactory msgFactory = servCtx.getMessageFactory();

			final int messagePart = params.getIntParameter(
					PARAM_NAME_MESSAGE_PART, XQConstants.PARAM_STRING);

			final boolean keepOriginalPart = params.getBooleanParameter(
					PARAM_NAME_KEEP_ORIGINAL_PART, XQConstants.PARAM_STRING);

			if (direction.equals(DIRECTION_FROM) && format.equals(FORMAT_GZIP)) {

				while (servCtx.hasNextIncoming()) {
					final XQEnvelope env = servCtx.getNextIncoming();

					final XQMessage origMsg = env.getMessage();

					final Iterator addressIterator = env.getAddresses();

					for (int i = 0; i < origMsg.getPartCount(); i++) {

						/* Decide whether to process the part or not */
						if ((messagePart == i)
								|| (messagePart == XQConstants.ALL_PARTS)) {
							final XQMessage newMsg = msgFactory.createMessage();

							/*
							 * Copy all headers from the original message to the
							 * new message
							 */
							MessageUtils.copyAllHeaders(origMsg, newMsg);

							final XQPart origPart = origMsg.getPart(i);

							/* Decide whether to keep the original part or not */
							if (keepOriginalPart) {
								origPart.setContentId("original_part_" + i);

								newMsg.addPart(origPart);
							}

							final XQPart newPart = newMsg.createPart();

							newPart.setContentId("DecompressedMessage-" + i);

							final byte[] content = (byte[]) origPart
									.getContent();

							final InputStream in = new GZIPInputStream(
									new ByteArrayInputStream(content));

							final byte[] buf = new byte[128];

							int count = in.read(buf);

							final StringBuilder builder = new StringBuilder();

							while (count != -1) {
								builder.append(new String(buf, 0, count));

								count = in.read(buf);
							}

							in.close();

							newPart.setContent(builder.toString(),
									XQConstants.CONTENT_TYPE_XML);

							newMsg.addPart(newPart);

							env.setMessage(newMsg);

							if (addressIterator.hasNext())
								servCtx.addOutgoing(env);

						}

						/* Break when done */
						if (messagePart == i)
							break;

					}

				}

			} else if (direction.equals(DIRECTION_FROM)
					&& format.equals(FORMAT_ZIP)) {

				while (servCtx.hasNextIncoming()) {
					final XQEnvelope env = servCtx.getNextIncoming();

					final XQMessage origMsg = env.getMessage();

					final Iterator addressIterator = env.getAddresses();

					for (int i = 0; i < origMsg.getPartCount(); i++) {

						/* Decide whether to process the part or not */
						if ((messagePart == i)
								|| (messagePart == XQConstants.ALL_PARTS)) {
							final XQMessage newMsg = msgFactory.createMessage();

							/*
							 * Copy all headers from the original message to the
							 * new message
							 */
							MessageUtils.copyAllHeaders(origMsg, newMsg);

							final XQPart origPart = origMsg.getPart(i);

							/* Decide whether to keep the original part or not */
							if (keepOriginalPart) {
								origPart.setContentId("original_part_" + i);

								newMsg.addPart(origPart);
							}

							final byte[] content = (byte[]) origPart
									.getContent();

							final ZipInputStream in = new ZipInputStream(
									new BufferedInputStream(
											new ByteArrayInputStream(content)));

							ZipEntry nextEntry = in.getNextEntry();

							int entryIndex = 0;

							while (nextEntry != null) {
								final XQPart newPart = newMsg.createPart();

								newPart.setContentId("DecompressedMessage-" + i
										+ "_" + entryIndex++);

								final byte[] buf = new byte[128];

								int count = in.read(buf);

								final StringBuilder builder = new StringBuilder();

								while (count != -1) {
									builder.append(new String(buf, 0, count));

									count = in.read(buf);
								}

								newPart.setContent(builder.toString(),
										XQConstants.CONTENT_TYPE_XML);

								newMsg.addPart(newPart);

								nextEntry = in.getNextEntry();
							}

							in.close();

							env.setMessage(newMsg);

							if (addressIterator.hasNext())
								servCtx.addOutgoing(env);

						}

						/* Break when done */
						if (messagePart == i)
							break;

					}

				}

			} else if (direction.equals(DIRECTION_TO)
					&& format.equals(FORMAT_GZIP)) {

				while (servCtx.hasNextIncoming()) {
					final XQEnvelope env = servCtx.getNextIncoming();

					final XQMessage origMsg = env.getMessage();

					final Iterator addressIterator = env.getAddresses();

					for (int i = 0; i < origMsg.getPartCount(); i++) {

						/* Decide whether to process the part or not */
						if ((messagePart == i)
								|| (messagePart == XQConstants.ALL_PARTS)) {
							final XQMessage newMsg = msgFactory.createMessage();

							/*
							 * Copy all headers from the original message to the
							 * new message
							 */
							MessageUtils.copyAllHeaders(origMsg, newMsg);

							final XQPart origPart = origMsg.getPart(i);

							/* Decide whether to keep the original part or not */
							if (keepOriginalPart) {
								origPart.setContentId("original_part_" + i);

								newMsg.addPart(origPart);
							}

							final XQPart newPart = newMsg.createPart();

							newPart.setContentId("CompressedMessage-" + i);

							final String content = (String) origPart
									.getContent();

							final ByteArrayOutputStream baseOut = new ByteArrayOutputStream(
									content.length());

							final OutputStream gzipOut = new GZIPOutputStream(
									baseOut);

							gzipOut.write(content.getBytes());

							gzipOut.close();

							newPart.setContent(baseOut.toByteArray(),
									XQConstants.CONTENT_TYPE_BYTES);

							newMsg.addPart(newPart);

							env.setMessage(newMsg);

							if (addressIterator.hasNext())
								servCtx.addOutgoing(env);

						}

						/* Break when done */
						if (messagePart == i)
							break;

					}

				}

			} else if (direction.equals(DIRECTION_TO)
					&& format.equals(FORMAT_ZIP)) {

				while (servCtx.hasNextIncoming()) {
					final XQEnvelope env = servCtx.getNextIncoming();

					final XQMessage origMsg = env.getMessage();

					final Iterator addressIterator = env.getAddresses();

					for (int i = 0; i < origMsg.getPartCount(); i++) {

						/* Decide whether to process the part or not */
						if ((messagePart == i)
								|| (messagePart == XQConstants.ALL_PARTS)) {
							final XQMessage newMsg = msgFactory.createMessage();

							/*
							 * Copy all headers from the original message to the
							 * new message
							 */
							MessageUtils.copyAllHeaders(origMsg, newMsg);

							final XQPart origPart = origMsg.getPart(i);

							/* Decide whether to keep the original part or not */
							if (keepOriginalPart) {
								origPart.setContentId("original_part_" + i);

								newMsg.addPart(origPart);
							}

							final XQPart newPart = newMsg.createPart();

							newPart.setContentId("CompressedMessage-" + i);

							final String content = (String) origPart
									.getContent();

							final ByteArrayOutputStream baseOut = new ByteArrayOutputStream(
									content.length());

							final ZipOutputStream zipOut = new ZipOutputStream(
									new BufferedOutputStream(baseOut));

							zipOut.putNextEntry(new ZipEntry("Entry-0"));

							zipOut.write(content.getBytes());

							zipOut.close();

							newPart.setContent(baseOut.toByteArray(),
									XQConstants.CONTENT_TYPE_BYTES);

							newMsg.addPart(newPart);

							env.setMessage(newMsg);

							if (addressIterator.hasNext())
								servCtx.addOutgoing(env);

						}

						/* Break when done */
						if (messagePart == i)
							break;

					}

				}

			}

		} catch (final Exception e) {
			throw new XQServiceException(e);
		}

	}

}
