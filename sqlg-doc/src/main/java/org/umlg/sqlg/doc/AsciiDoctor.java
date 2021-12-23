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
//        String version = "2.0.1";
        String version = "2.1.6";
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
            options.put("location", ":footer");
            Docinfo docinfo = new Docinfo(options);
            asciidoctor.javaExtensionRegistry().docinfoProcessor(docinfo);
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
            return "<script src=\"tocbot.min.js\"></script>\n" +
                    "<link rel=\"stylesheet\" href=\"https://cdnjs.cloudflare.com/ajax/libs/tocbot/4.11.1/tocbot.css\">\n" +
                    "<script>\n" +
                    "    var oldtoc = document.getElementById('toctitle').nextElementSibling;\n" +
                    "    var newtoc = document.createElement('div');\n" +
                    "    newtoc.setAttribute('id', 'tocbot');\n" +
                    "    newtoc.setAttribute('class', 'js-toc');\n" +
                    "    oldtoc.parentNode.replaceChild(newtoc, oldtoc);\n" +
                    "    tocbot.init({contentSelector: '#content', headingSelector: 'h1, h2, h3, h4, h5', smoothScroll: true});\n" +
                    "    var handleTocOnResize = function () {\n" +
                    "        var width = window.innerWidth || document.documentElement.clientWidth || document.body.clientWidth;\n" +
                    "        if (width < 768) {\n" +
                    "            tocbot.refresh({\n" +
                    "                contentSelector: '#content',\n" +
                    "                headingSelector: 'h1, h2, h3, h4, h5',\n" +
                    "                collapseDepth: 6,\n" +
                    "                activeLinkClass: 'ignoreactive',\n" +
                    "                throttleTimeout: 1000,\n" +
                    "                smoothScroll: false\n" +
                    "            });\n" +
                    "        } else {\n" +
                    "            tocbot.refresh({\n" +
                    "                contentSelector: '#content',\n" +
                    "                headingSelector: 'h1, h2, h3, h4, h5',\n" +
                    "                smoothScroll: false\n" +
                    "            });\n" +
                    "        }\n" +
                    "    };\n" +
                    "    window.addEventListener('resize', handleTocOnResize);\n" +
                    "    handleTocOnResize();\n" +
                    "</script>\n";
        }
    }
}
