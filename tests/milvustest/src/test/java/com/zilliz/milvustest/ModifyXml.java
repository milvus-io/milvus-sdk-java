package com.zilliz.milvustest;

import org.w3c.dom.*;
import org.xml.sax.SAXException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import java.io.*;

/**
 * @Author yongpeng.li
 * @Date 2023/3/7 17:38
 */
public class ModifyXml {
    private static final String FILENAME = "../../../pom.xml";
    // xslt for pretty print only, no special task
    private static final String FORMAT_XSLT = "../../../pom.xslt";
    public static void main(String[] args) {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        try (InputStream is = new FileInputStream(FILENAME)) {
            DocumentBuilder db = dbf.newDocumentBuilder();
            Document doc = db.parse(is);
            NodeList dependencyList = doc.getElementsByTagName("dependency");
           int num=0;
            for (int i = 0; i < dependencyList.getLength(); i++) {
                Node dependencyItem = dependencyList.item(i);
                NodeList childNodes = dependencyItem.getChildNodes();
                for (int j = 0; j < childNodes.getLength(); j++) {
                    Node item = childNodes.item(j);
                    if ("artifactId".equalsIgnoreCase(item.getNodeName())&&item.getTextContent().equalsIgnoreCase("milvus-sdk-java")) {
                       num=i;
                       break;
                    }
                }
            }
            NodeList childNodes = dependencyList.item(num).getChildNodes();
            for (int i = 0; i < childNodes.getLength(); i++) {
                 Node item = childNodes.item(i);
                if ("version".equalsIgnoreCase(item.getNodeName())) {
                    item.setTextContent(args[0]);
                }
            }
            try (FileOutputStream output =
                         new FileOutputStream(FILENAME)) {
                writeXml(doc, output);
            } catch (TransformerException e) {
                throw new RuntimeException(e);
            }

        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ParserConfigurationException e) {
            throw new RuntimeException(e);
        } catch (SAXException e) {
            throw new RuntimeException(e);
        }
        }


        // write doc to output stream
    private static void writeXml(Document doc,
                                 OutputStream output)
            throws TransformerException, UnsupportedEncodingException {
        TransformerFactory transformerFactory = TransformerFactory.newInstance();
        Transformer transformer = transformerFactory.newTransformer(
                new StreamSource(new File(FORMAT_XSLT)));

        transformer.setOutputProperty(OutputKeys.INDENT, "yes");
        transformer.setOutputProperty(OutputKeys.STANDALONE, "no");
        DOMSource source = new DOMSource(doc);
        StreamResult result = new StreamResult(output);
        transformer.transform(source, result);
    }
}
