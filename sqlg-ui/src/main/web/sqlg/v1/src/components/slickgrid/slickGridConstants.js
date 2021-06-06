import Utils from "../../utils/utils";

const SlickGridConstants = {

    COLUMN_TYPE: {
        STRING: { name: "STRING", isString() { return true }, isNumber() { return false; }, defaultSearchEnum: Utils.SEARCH_ENUM.contains},
        STRING_ARRAY: { name: "STRING_ARRAY", isString() { return true }, isNumber() { return false; }, defaultSearchEnum: Utils.SEARCH_ENUM.contains},
        SHORT: { name: "SHORT", isString() { return false }, isNumber() { return true; }, defaultSearchEnum: Utils.SEARCH_ENUM.equals },
        INTEGER: { name: "INTEGER", isString() { return false }, isNumber() { return true; }, defaultSearchEnum: Utils.SEARCH_ENUM.equals },
        LONG: { name: "LONG", isString() { return false }, isNumber() { return true; }, defaultSearchEnum: Utils.SEARCH_ENUM.equals },
        FLOAT: { name: "FLOAT", isString() { return false }, isNumber() { return true; }, defaultSearchEnum: Utils.SEARCH_ENUM.equals },
        DOUBLE: { name: "DOUBLE", isString() { return false }, isNumber() { return true; }, defaultSearchEnum: Utils.SEARCH_ENUM.equals },
        NUMBER: { name: "NUMBER", isString() { return false }, isNumber() { return true; }, defaultSearchEnum: Utils.SEARCH_ENUM.equals },
    },

    fromName: (name) => {
        switch (name) {
            case "STRING":
                return SlickGridConstants.COLUMN_TYPE.STRING;
            case "STRING_ARRAY":
                return SlickGridConstants.COLUMN_TYPE.STRING_ARRAY;
            case "SHORT":
                return SlickGridConstants.COLUMN_TYPE.SHORT;
            case "INTEGER":
                return SlickGridConstants.COLUMN_TYPE.INTEGER;
            case "LONG":
                return SlickGridConstants.COLUMN_TYPE.LONG;
            case "FLOAT":
                return SlickGridConstants.COLUMN_TYPE.FLOAT;
            case "DOUBLE":
                return SlickGridConstants.COLUMN_TYPE.DOUBLE;
            case "NUMBER":
                return SlickGridConstants.COLUMN_TYPE.NUMBER;
            default:
                throw new Error("Unknown SlickGridConstants.COLUMN_TYPE '" + name + "'");
        }
    }

}
export default SlickGridConstants;
