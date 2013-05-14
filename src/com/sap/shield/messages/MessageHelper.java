package com.sap.shield.messages;

import org.w3c.dom.Document;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.StringWriter;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 10/25/12
 * Time: 9:20 PM
 * To change this template use File | Settings | File Templates.
 */
public class MessageHelper {

    private static final Logger LOG = Logger.getLogger(MessageHelper.class.getName());

    public static final DocumentBuilder DOCUMENT_BUILDER = getDocumentBuilder();

    /**
     * Returns a default DocumentBuilder instance or throws an
     * ExceptionInInitializerError if it can't be created.
     *
     * @return a default DocumentBuilder instance.
     */
    public static DocumentBuilder getDocumentBuilder() {
        try {
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            dbf.setIgnoringComments(true);
            dbf.setCoalescing(true);
            dbf.setIgnoringElementContentWhitespace(true);
            dbf.setValidating(false);
            return dbf.newDocumentBuilder();
        } catch (Exception exc) {
            exc.printStackTrace();
            LOG.exiting(MessageHelper.class.getName(), "getDocumentBuilder()", exc);
            return null;
        }
    }

    public static String getXmlString(Document document) {
        try {
            Transformer transformer = TransformerFactory.newInstance().newTransformer();
            StreamResult result = new StreamResult(new StringWriter());
            DOMSource source = new DOMSource(document);
            transformer.transform(source, result);
            return result.getWriter().toString();
        } catch (TransformerException ex) {
            LOG.exiting(MessageHelper.class.getName(), "getXmlString()", ex);
            ex.printStackTrace();
            return null;
        }
    }
}
