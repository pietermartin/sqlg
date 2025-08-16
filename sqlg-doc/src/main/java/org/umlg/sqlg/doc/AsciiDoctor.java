package org.umlg.sqlg.doc;

import org.asciidoctor.Asciidoctor;
import org.asciidoctor.Attributes;
import org.asciidoctor.Options;
import org.asciidoctor.SafeMode;
import org.asciidoctor.ast.Document;
import org.asciidoctor.extension.DocinfoProcessor;
import org.asciidoctor.extension.Location;
import org.asciidoctor.extension.LocationType;

import java.io.File;

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
        String version = "3.1.3";
        try (Asciidoctor asciidoctor = create()) {
            File file = new File("sqlg-doc/docs/" + version + "/sqlg.adoc");
            File html = new File("sqlg-doc/docs/" + version + "/index.html");
            Attributes attributes = Attributes.builder()
                    .styleSheetName("asciidoctor-default.css")
//                    .styleSheetName("boot-superhero.css")
                    .docType("book")
                    .backend("html5")
                    .sourceHighlighter("highlightjs")
                    .build();
            Options options = Options.builder()
                    .attributes(attributes)
//                    .headerFooter(true)
                    .toFile(new File(html.getPath()))
                    .safe(SafeMode.SERVER)
                    .build();
            DocinfoHeader docinfoHeader = new DocinfoHeader();
            DocinfoFooter docinfoFooter = new DocinfoFooter();
            asciidoctor.javaExtensionRegistry()
                    .docinfoProcessor(docinfoHeader)
                    .docinfoProcessor(docinfoFooter);
            asciidoctor.convertFile(
                    file,
                    options
            );
        }
    }
    @Location(LocationType.HEADER)
    private class DocinfoHeader extends DocinfoProcessor {

        @Override
        public String process(Document document) {
            return "<script src=\"tocbot.min.js\"></script>\n<link rel=\"stylesheet\" href=\"tocbot.css\">";
        }
    }

    @Location(LocationType.FOOTER)
    private class DocinfoFooter extends DocinfoProcessor {

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
