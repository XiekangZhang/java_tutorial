package de.xiekang.talend;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import java.io.File;
import java.io.IOException;

public class HTMLExtract {
    Document document;

    public HTMLExtract() {
        try {
            document = Jsoup.parse(new File(HTMLExtract.class.getResource("/test.html").getFile()));
        } catch (IOException e) {
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
        HTMLExtract htmlExtract = new HTMLExtract();
        Document d = htmlExtract.getDocument();
        Elements element = d.select("form[id='kc-form-login']");
        System.out.println(element);
        System.out.println(element.attr("action"));
    }
}
