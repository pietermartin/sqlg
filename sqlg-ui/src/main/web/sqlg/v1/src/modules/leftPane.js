import m from "mithril";
import SchemaTree from "./schema/schemaTree";

function LeftPane(ignore) {

    return {
        view: () => {
            return m(SchemaTree)
        }
    }

}

export default LeftPane;