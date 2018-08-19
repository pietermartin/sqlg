package org.umlg.sqlg.doc;

import org.asciidoctor.Asciidoctor;
import org.asciidoctor.Attributes;
import org.asciidoctor.OptionsBuilder;
import org.asciidoctor.SafeMode;
import org.asciidoctor.ast.Document;
import org.asciidoctor.extension.DocinfoProcessor;

import java.io.File;
import java.util.Map;

import static org.asciidoctor.Asciidoctor.Factory.create;

/**
 * Date: 2016/12/14
 * Time: 1:43 PM
 */
public class AsciiDoctor {

    public static void main(String[] args) {
        new AsciiDoctor().createDocs();
    }

    private void createDocs() {
        String version = "2.0.0-SNAPSHOT";
        Asciidoctor asciidoctor = create();
        try {
            File file = new File("sqlg-doc/docs/" + version + "/sqlg.adoc");
            File html = new File("sqlg-doc/docs/" + version + "/index.html");
            Attributes attributes = new Attributes();
            attributes.setBackend("html5");
            attributes.setStyleSheetName("asciidoctor-default.css");
            attributes.setDocType("book");
            attributes.setSourceHighlighter("highlightjs");

            Map<String, Object> options =  OptionsBuilder.options()
                    .attributes(attributes)
                    .toFile(new File(html.getPath()))
                    .headerFooter(true)
                    .safe(SafeMode.SERVER)
                    .asMap();
//            options.put("location", ":footer");
            Docinfo docinfo = new Docinfo(options);
//            asciidoctor.javaExtensionRegistry().docinfoProcessor(docinfo);
            asciidoctor.convertFile(
                    file,
                    options
            );
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private class Docinfo extends DocinfoProcessor {

        Docinfo(Map<String, Object> config) {
            super(config);
        }


        @Override
        public String process(Document document) {
            return "<script src=\"https://cdnjs.cloudflare.com/ajax/libs/tocbot/4.1.1/tocbot.min.js\"></script>\n" +
                    "<link rel=\"stylesheet\" href=\"https://cdnjs.cloudflare.com/ajax/libs/tocbot/4.1.1/tocbot.css\">" +
                    "<script>tocbot.init({\n" +
                    "  // Where to render the table of contents.\n" +
                    "  tocSelector: '.js-toc',\n" +
                    "  // Where to grab the headings to build the table of contents.\n" +
                    "  contentSelector: '.js-toc-content',\n" +
                    "  // Which headings to grab inside of the contentSelector element.\n" +
                    "  headingSelector: 'h1, h2, h3',\n" +
                    "});</script>";
        }
    }
}
