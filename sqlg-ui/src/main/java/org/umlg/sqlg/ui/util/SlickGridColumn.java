package org.umlg.sqlg.ui.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.StringUtils;
import org.umlg.sqlg.structure.PropertyType;

import java.util.HashMap;
import java.util.Map;

public class SlickGridColumn {

    private final String id;
    private final String field;
    private final String name;
    private final PropertyType type;
    private final boolean editable;
    private final boolean required;
    private final Map<String, String> options;
    private final Color color;
    private final int width;
    private final int minWidth;
    private final int maxWidth;
    private final Map<Integer, Color> cellColorMap;
    private final String icon;
    private final boolean isSingleSelect;
    private final String columnGroup;

    private SlickGridColumn(SlickGridColumnBuilder slickGridColumnBuilder) {
        this.id = slickGridColumnBuilder.id;
        this.field = slickGridColumnBuilder.field;
        this.name = slickGridColumnBuilder.name;
        this.type = slickGridColumnBuilder.type;
        this.isSingleSelect = slickGridColumnBuilder.isSingleSelect;
        this.editable = slickGridColumnBuilder.editable;
        this.required = slickGridColumnBuilder.required;
        this.options = slickGridColumnBuilder.options;
        this.color = slickGridColumnBuilder.color;
        this.width = slickGridColumnBuilder.width;
        this.minWidth = slickGridColumnBuilder.minWidth;
        this.maxWidth = slickGridColumnBuilder.maxWidth;
        this.cellColorMap = slickGridColumnBuilder.cellColorMap;
        this.icon = slickGridColumnBuilder.icon;
        this.columnGroup = slickGridColumnBuilder.columnGroup;
    }

    public ObjectNode toJson(ObjectMapper objectMapper) {
        ObjectNode json = objectMapper.createObjectNode();
        json.put("id", this.id);
        json.put("field", this.field);
        json.put("name", this.name);

        ObjectNode columnAdvanced = objectMapper.createObjectNode();
        columnAdvanced.put("type", (isSingleSelect ? "SINGLE_SELECT" : this.type.name()));
        columnAdvanced.put("isEditable", this.editable);
        columnAdvanced.put("isRequired", this.required);
        if (this.options != null) {
            ArrayNode optionsJson = objectMapper.createArrayNode();
            for (String option : this.options.keySet()) {
                ObjectNode optionObjectNode = objectMapper.createObjectNode();
                optionObjectNode.put("id", option);
                optionObjectNode.put("name", this.options.get(option));
                optionsJson.add(optionObjectNode);
            }
            columnAdvanced.set("options", optionsJson);
        }
        if (!this.cellColorMap.isEmpty()) {
            ObjectNode cellColorMap = objectMapper.createObjectNode();
            for (Integer index : this.cellColorMap.keySet()) {
                Color color = this.cellColorMap.get(index);
                cellColorMap.put(index.toString(), color.getCssClass());
            }
            columnAdvanced.set("cellColorMap", cellColorMap);
        }
        if (this.type == PropertyType.BOOLEAN) {
            json.put("minWidth", 60);
        }
        if (this.color != null) {
            json.put("cssClass", this.color.cssClass);
        }
        if (this.width != -1) {
            json.put("width", this.width);
        }
        if (this.minWidth != -1) {
            json.put("minWidth", this.minWidth);
        }
        if (this.maxWidth != -1) {
            json.put("maxWidth", this.maxWidth);
        }
        if (!StringUtils.isEmpty(this.icon)) {
            columnAdvanced.put("icon", this.icon);
        }
        if (!StringUtils.isEmpty(this.columnGroup)) {
            json.put("columnGroup", this.columnGroup);
        }
        json.set("cm_column", columnAdvanced);
        return json;
    }

    public static class SlickGridColumnBuilder {
        private final String id;
        private final String field;
        private final String name;
        private final PropertyType type;
        private boolean isSingleSelect;
        private boolean editable = false;
        private boolean required = false;
        private Map<String, String> options;
        private Color color;
        private int width = -1;
        private int minWidth = -1;
        private int maxWidth = -1;
        private Map<Integer, Color> cellColorMap = new HashMap<>();
        private String icon;
        private String columnGroup;

        public SlickGridColumnBuilder(String id, String name, PropertyType type) {
            this(id, id, name, type);
        }

        SlickGridColumnBuilder(String id, String field, String name, PropertyType type) {
            this.id = id;
            this.field = field;
            this.name = name;
            this.type = type;
        }

        public SlickGridColumnBuilder singleSelect() {
            this.isSingleSelect = true;
            return this;
        }

        public SlickGridColumnBuilder setOptions(Map<String, String> options) {
            this.options = options;
            return this;
        }

        public SlickGridColumnBuilder setEditable(boolean editable) {
            this.editable = editable;
            return this;
        }

        public SlickGridColumnBuilder setRequired(boolean required) {
            this.required = required;
            return this;
        }

        public SlickGridColumnBuilder setColor(Color color) {
            this.color = color;
            return this;
        }

        public SlickGridColumnBuilder setWidth(int width) {
            this.width = width;
            return this;
        }

        public SlickGridColumnBuilder setMinWidth(int minWidth) {
            this.minWidth = minWidth;
            return this;
        }

        public SlickGridColumnBuilder setMaxWidth(int maxWidth) {
            this.maxWidth = maxWidth;
            return this;
        }

        public SlickGridColumnBuilder setCellColorMap(Map<Integer, Color> cellColorMap) {
            this.cellColorMap = cellColorMap;
            return this;
        }

        public SlickGridColumnBuilder setIcon(String icon) {
            this.icon = icon;
            return this;
        }

        public SlickGridColumnBuilder setColumnGroup(String columnGroup) {
            this.columnGroup = columnGroup;
            return this;
        }

        public SlickGridColumn build() {
            return new SlickGridColumn(this);
        }
    }

    public enum Color {
        PINK_BG_005("slick-cell-bg-pink-005", new int[]{255, 0, 128, 12}, true),
        PINK_BG_05("slick-cell-bg-pink-05", new int[]{255, 0, 128, 127}, true),
        PINK_BG_01("slick-cell-bg-pink-01", new int[]{255, 0, 128, 27}, true),
        PINK_BG_03("slick-cell-bg-pink-03", new int[]{255, 0, 128, 76}, true),
        PINK_BG_1("slick-cell-bg-pink-1", new int[]{255, 0, 128, 1}, true),
        PINK_005("slick-cell-pink-005", new int[]{255, 0, 128, 12}, false),
        PINK_05("slick-cell-pink-05", new int[]{255, 0, 128, 127}, false),
        PINK_01("slick-cell-pink-01", new int[]{255, 0, 128, 27}, false),
        PINK_03("slick-cell-pink-03", new int[]{255, 0, 128, 76}, false),
        PINK_1("slick-cell-pink-1", new int[]{255, 0, 128, 1}, false),
        RED_BG_005("slick-cell-bg-red-005", new int[]{255, 0, 0, 12}, true),
        RED_BG_01("slick-cell-bg-red-01", new int[]{255, 0, 0, 27}, true),
        RED_BG_03("slick-cell-bg-red-03", new int[]{255, 0, 0, 76}, true),
        RED_BG_05("slick-cell-bg-red-05", new int[]{255, 0, 0, 127}, true),
        RED_BG_1("slick-cell-bg-red-1", new int[]{255, 0, 0, 1}, true),
        RED_005("slick-cell-red-005", new int[]{255, 0, 0, 12}, false),
        RED_01("slick-cell-red-01", new int[]{255, 0, 0, 27}, false),
        RED_03("slick-cell-red-03", new int[]{255, 0, 0, 76}, false),
        RED_05("slick-cell-red-05", new int[]{255, 0, 0, 127}, false),
        RED_1("slick-cell-red-1", new int[]{255, 0, 0, 1}, false),
        ORANGE_BG_005("slick-cell-bg-orange-005", new int[]{255, 128, 0, 12}, true),
        ORANGE_BG_01("slick-cell-bg-orange-01", new int[]{255, 128, 0, 27}, true),
        ORANGE_BG_03("slick-cell-bg-orange-03", new int[]{255, 128, 0, 76}, true),
        ORANGE_BG_05("slick-cell-bg-orange-05", new int[]{255, 128, 0, 127}, true),
        ORANGE_BG_1("slick-cell-bg-orange-1", new int[]{255, 128, 0, 1}, true),
        ORANGE_005("slick-cell-orange-005", new int[]{255, 128, 0, 12}, true),
        ORANGE_01("slick-cell-orange-01", new int[]{255, 128, 0, 27}, false),
        ORANGE_03("slick-cell-orange-03", new int[]{255, 128, 0, 76}, false),
        ORANGE_05("slick-cell-orange-05", new int[]{255, 128, 0, 127}, false),
        ORANGE_1("slick-cell-orange-1", new int[]{255, 128, 0, 1}, false),
        YELLOW_BG_005("slick-cell-bg-yellow-005", new int[]{255, 255, 0, 12}, true),
        YELLOW_BG_01("slick-cell-bg-yellow-01", new int[]{255, 255, 0, 27}, true),
        YELLOW_BG_03("slick-cell-bg-yellow-03", new int[]{255, 255, 0, 76}, true),
        YELLOW_BG_05("slick-cell-bg-yellow-05", new int[]{255, 255, 0, 127}, true),
        YELLOW_BG_1("slick-cell-bg-yellow-1", new int[]{255, 255, 0, 1}, true),
        YELLOW_005("slick-cell-yellow-005", new int[]{255, 255, 0, 12}, false),
        YELLOW_01("slick-cell-yellow-01", new int[]{255, 255, 0, 27}, false),
        YELLOW_03("slick-cell-yellow-03", new int[]{255, 255, 0, 76}, false),
        YELLOW_05("slick-cell-yellow-05", new int[]{255, 255, 0, 127}, false),
        YELLOW_1("slick-cell-yellow-1", new int[]{255, 255, 0, 1}, false),
        GREEN_BG_005("slick-cell-bg-green-005", new int[]{0, 255, 0, 12}, true),
        GREEN_BG_01("slick-cell-bg-green-01", new int[]{0, 255, 0, 27}, true),
        GREEN_BG_03("slick-cell-bg-green-03", new int[]{0, 255, 0, 76}, true),
        GREEN_BG_05("slick-cell-bg-green-05", new int[]{0, 255, 0, 127}, true),
        GREEN_BG_1("slick-cell-bg-green-1", new int[]{0, 255, 0, 1}, true),
        GREEN_005("slick-cell-green-005", new int[]{0, 255, 0, 12}, false),
        GREEN_01("slick-cell-green-01", new int[]{0, 255, 0, 27}, false),
        GREEN_03("slick-cell-green-03", new int[]{0, 255, 0, 76}, false),
        GREEN_05("slick-cell-green-05", new int[]{0, 255, 0, 127}, false),
        GREEN_1("slick-cell-green-1", new int[]{0, 255, 0, 1}, false),
        TEAL_BG_005("slick-cell-bg-teal-005", new int[]{0, 255, 128, 12}, true),
        TEAL_BG_01("slick-cell-bg-teal-01", new int[]{0, 255, 128, 27}, true),
        TEAL_BG_03("slick-cell-bg-teal-03", new int[]{0, 255, 128, 76}, true),
        TEAL_BG_05("slick-cell-bg-teal-05", new int[]{0, 255, 128, 127}, true),
        TEAL_BG_1("slick-cell-bg-teal-1", new int[]{0, 255, 128, 1}, true),
        TEAL_005("slick-cell-teal-005", new int[]{0, 255, 128, 12}, false),
        TEAL_01("slick-cell-teal-01", new int[]{0, 255, 128, 27}, false),
        TEAL_03("slick-cell-teal-03", new int[]{0, 255, 128, 76}, false),
        TEAL_05("slick-cell-teal-05", new int[]{0, 255, 128, 127}, false),
        TEAL_1("slick-cell-teal-1", new int[]{0, 255, 128, 1}, false),
        CYAN_BG_005("slick-cell-bg-cyan-005", new int[]{0, 255, 255, 12}, true),
        CYAN_BG_01("slick-cell-bg-cyan-01", new int[]{0, 255, 255, 27}, true),
        CYAN_BG_03("slick-cell-bg-cyan-03", new int[]{0, 255, 255, 76}, true),
        CYAN_BG_05("slick-cell-bg-cyan-05", new int[]{0, 255, 255, 127}, true),
        CYAN_BG_1("slick-cell-bg-cyan-1", new int[]{0, 255, 255, 1}, true),
        CYAN_005("slick-cell-cyan-005", new int[]{0, 255, 255, 12}, false),
        CYAN_01("slick-cell-cyan-01", new int[]{0, 255, 255, 27}, false),
        CYAN_03("slick-cell-cyan-03", new int[]{0, 255, 255, 76}, false),
        CYAN_05("slick-cell-cyan-05", new int[]{0, 255, 255, 127}, false),
        CYAN_1("slick-cell-cyan-1", new int[]{0, 255, 255, 1}, false),
        BLUE_BG_005("slick-cell-bg-blue-005", new int[]{0, 0, 255, 12}, true),
        BLUE_BG_01("slick-cell-bg-blue-01", new int[]{0, 0, 255, 27}, true),
        BLUE_BG_03("slick-cell-bg-blue-03", new int[]{0, 0, 255, 76}, true),
        BLUE_BG_05("slick-cell-bg-blue-05", new int[]{0, 0, 255, 127}, true),
        BLUE_BG_1("slick-cell-bg-blue-1", new int[]{0, 0, 255, 1}, true),
        BLUE_005("slick-cell-blue-005", new int[]{0, 0, 255, 12}, false),
        BLUE_01("slick-cell-blue-01", new int[]{0, 0, 255, 27}, false),
        BLUE_03("slick-cell-blue-03", new int[]{0, 0, 255, 76}, false),
        BLUE_05("slick-cell-blue-05", new int[]{0, 0, 255, 127}, false),
        BLUE_1("slick-cell-blue-1", new int[]{0, 0, 255, 1}, false),
        PURPLE_BG_005("slick-cell-bg-purple-005", new int[]{128, 0, 255, 12}, true),
        PURPLE_BG_01("slick-cell-bg-purple-01", new int[]{128, 0, 255, 27}, true),
        PURPLE_BG_03("slick-cell-bg-purple-03", new int[]{128, 0, 255, 76}, true),
        PURPLE_BG_05("slick-cell-bg-purple-05", new int[]{128, 0, 255, 127}, true),
        PURPLE_BG_1("slick-cell-bg-purple-1", new int[]{128, 0, 255, 1}, true),
        PURPLE_005("slick-cell-purple-005", new int[]{128, 0, 255, 12}, false),
        PURPLE_01("slick-cell-purple-01", new int[]{128, 0, 255, 27}, false),
        PURPLE_03("slick-cell-purple-03", new int[]{128, 0, 255, 76}, false),
        PURPLE_05("slick-cell-purple-05", new int[]{128, 0, 255, 127}, false),
        PURPLE_1("slick-cell-purple-1", new int[]{128, 0, 255, 1}, false),
        BLACK_BG_005("slick-cell-bg-black-005", new int[]{0, 0, 0, 12}, true),
        BLACK_BG_01("slick-cell-bg-black-01", new int[]{0, 0, 0, 27}, true),
        BLACK_BG_03("slick-cell-bg-black-03", new int[]{0, 0, 0, 76}, true),
        BLACK_BG_05("slick-cell-bg-black-05", new int[]{0, 0, 0, 127}, true),
        BLACK_BG_1("slick-cell-bg-black-1", new int[]{0, 0, 0, 1}, true),
        BLACK_005("slick-cell-black-005", new int[]{0, 0, 0, 12}, false),
        BLACK_01("slick-cell-black-01", new int[]{0, 0, 0, 27}, false),
        BLACK_03("slick-cell-black-03", new int[]{0, 0, 0, 76}, false),
        BLACK_05("slick-cell-black-05", new int[]{0, 0, 0, 127}, false),
        BLACK_1("slick-cell-black-1", new int[]{0, 0, 0, 1}, false);

        private final String cssClass;
        private final int[] rgba;
        private final boolean isBackground;

        Color(String cssClass, int[] rgba, boolean isBackground) {
            this.cssClass = cssClass;
            this.rgba = rgba;
            this.isBackground = isBackground;
        }

        public String getCssClass() {
            return cssClass;
        }

        public int[] getRgba() {
            return rgba;
        }

        public java.awt.Color getColor() {
            return new java.awt.Color(rgba[0], rgba[1], rgba[2], rgba[3]);
        }

//        public static Map<Color, XSSFCellStyle> xlsStyles(SXSSFWorkbook workbook) {
//            Map<Color, XSSFCellStyle> styles = new HashMap<>();
//            for (Color color : values()) {
//                XSSFColor xssfColor = new XSSFColor(color.getColor(), null);
//                XSSFCellStyle style = (XSSFCellStyle) workbook.createCellStyle();
//                if (color.isBackground) {
//                    style.setFillForegroundColor(xssfColor);
//                    style.setFillPattern(FillPatternType.SOLID_FOREGROUND);
//                } else {
//                    XSSFFont font = (XSSFFont) workbook.createFont();
//                    font.setColor(xssfColor);
//                    style.setFont(font);
//                }
//                styles.put(color, style);
//            }
//            return styles;
//        }
    }

    public static int rowNumMaxWidth(int size) {
        if (size < 100) {
            return 70;
        } else if (size < 1000) {
            return 75;
        } else if (size < 10000) {
            return 85;
        } else if (size < 100000) {
            return 90;
        } else if (size < 1000000) {
            return 95;
        } else if (size < 10000000) {
            return 100;
        } else {
            return 105;
        }
    }
}