package de.xiekang.talend;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.io.DOMReader;
import org.dom4j.io.SAXReader;

public class XMLExtract {
    Document document;

    public XMLExtract() {
        SAXReader saxReader = new SAXReader();
        try {
            document = saxReader.read(XMLExtract.class.getResourceAsStream("/test.html"));
        } catch (DocumentException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    public Document getDocument() {
        return document;
    }

    public void setDocument(Document document) {
        this.document = document;
    }

    public static void main(String... args) {
        XMLExtract xmlExtract = new XMLExtract();
        Document document1 = xmlExtract.getDocument();
        System.out.println(document1.selectNodes("//form[@id='kc-form-login']/@action"));
    }
}
