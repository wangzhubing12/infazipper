package com.wzb.infa.obj;

import java.util.HashMap;

import org.dom4j.DocumentHelper;
import org.dom4j.Element;

import com.wzb.infa.exceptions.UnsupportedDatatypeException;

public class InfaCol {

    /**
     * *
     * 创建一个targetField对象
     *
     * @param createWithPrimaryKey
     * @return
     */
    public Element createTargetField(boolean createWithPrimaryKey) {
        Element targetField = null;
        try {
            targetField = DocumentHelper.createElement("TARGETFIELD").addAttribute("BUSINESSNAME", "")
                    .addAttribute("DATATYPE", this.getDataType()).addAttribute("DESCRIPTION", "")
                    .addAttribute("FIELDNUMBER", this.columnId)
                    .addAttribute("KEYTYPE", createWithPrimaryKey ? this.keyType : "NOT A KEY")
                    .addAttribute("NAME", this.getColumnName()).addAttribute("NULLABLE", this.nullable)
                    .addAttribute("PICTURETEXT", "").addAttribute("PRECISION", this.getDataPrecision())
                    .addAttribute("SCALE", this.getDataScale());
        } catch (UnsupportedDatatypeException e) {
            System.out.println(e.getMessage());
        }

        return targetField;
    }

    /**
     * *
     * 创建一个updateStrategyField对象
     * @return 
     */
    public Element createUpdateStrategyField() {
        Element updateStrategyField = DocumentHelper.createElement("TRANSFORMFIELD")
                .addAttribute("DATATYPE", dataTypeO2I.getOrDefault(this.dataType, "string"))
                .addAttribute("DEFAULTVALUE", "").addAttribute("DESCRIPTION", "").addAttribute("GROUP", "INPUT")
                .addAttribute("NAME", this.getColumnName()).addAttribute("PICTURETEXT", "")
                .addAttribute("PORTTYPE", "INPUT/OUTPUT").addAttribute("PRECISION", this.getDataPrecision())
                .addAttribute("SCALE", this.getDataScale());
        return updateStrategyField;

    }

    /**
     * *
     * 创建一个joinerField对象
     * @param sufix
     * @param portType
     * @return 
     */
    public Element createJoinerField(String sufix, String portType) {
        Element joinerField = DocumentHelper.createElement("TRANSFORMFIELD")
                .addAttribute("DATATYPE", dataTypeO2I.getOrDefault(this.dataType, "string"))
                .addAttribute("DEFAULTVALUE", "").addAttribute("DESCRIPTION", "")
                .addAttribute("ISSORTKEY", "PRIMARY KEY".equals(this.keyType) ? "YES" : "NO")
                .addAttribute("NAME", this.columnName + sufix).addAttribute("PICTURETEXT", "")
                .addAttribute("PORTTYPE", portType).addAttribute("PRECISION", this.getPhysicalLength())
                .addAttribute("SCALE", this.getDataScale()).addAttribute("SORTDIRECTION", "ASCENDING");
        return joinerField;

    }

    /**
     * *
     * 创建一个sorterField对象
     * @return 
     */
    public Element createSorterField() {
        Element sorterField = DocumentHelper.createElement("TRANSFORMFIELD")
                .addAttribute("DATATYPE", dataTypeO2I.getOrDefault(this.dataType, "string"))
                .addAttribute("DEFAULTVALUE", "").addAttribute("DESCRIPTION", "")
                .addAttribute("ISSORTKEY", "PRIMARY KEY".equals(this.keyType) ? "YES" : "NO")
                .addAttribute("NAME", this.columnName).addAttribute("PICTURETEXT", "")
                .addAttribute("PORTTYPE", "INPUT/OUTPUT").addAttribute("PRECISION", this.getPhysicalLength())
                .addAttribute("SCALE", this.getDataScale()).addAttribute("SORTDIRECTION", "ASCENDING");
        return sorterField;

    }

    /**
     * *
     * 创建一个ExpressionField对象
     * @return 
     */
    public Element createExpressionField() {
        Element expressionField = DocumentHelper.createElement("TRANSFORMFIELD")
                .addAttribute("DATATYPE", dataTypeO2I.getOrDefault(this.dataType, "string"))
                .addAttribute("DEFAULTVALUE", "").addAttribute("DESCRIPTION", "")
                .addAttribute("EXPRESSION", this.columnName).addAttribute("EXPRESSIONTYPE", "GENERAL")
                .addAttribute("NAME", this.columnName).addAttribute("PICTURETEXT", "")
                .addAttribute("PORTTYPE", "INPUT/OUTPUT").addAttribute("PRECISION", this.getDataPrecision())
                .addAttribute("SCALE", this.getDataScale());
        return expressionField;
    }

    /**
     * *
     * 创建一个ExpressionField对象
     * @param expresion
     * @return 
     */
    public Element createExpressionField(String expresion) {
        Element expressionField = DocumentHelper.createElement("TRANSFORMFIELD")
                .addAttribute("DATATYPE", dataTypeO2I.getOrDefault(this.dataType, "string"))
                .addAttribute("DEFAULTVALUE", "").addAttribute("DESCRIPTION", "").addAttribute("EXPRESSION", expresion)
                .addAttribute("EXPRESSIONTYPE", "GENERAL").addAttribute("NAME", this.columnName)
                .addAttribute("PICTURETEXT", "").addAttribute("PORTTYPE", "OUTPUT")
                .addAttribute("PRECISION", this.getDataPrecision()).addAttribute("SCALE", this.getDataScale());
        return expressionField;
    }

    /**
     * *
     * 创建一个qualifierField对象
     * @return 
     */
    public Element createQualifierField() {
        Element qualifierField = DocumentHelper.createElement("TRANSFORMFIELD")
                .addAttribute("DATATYPE", dataTypeO2I.getOrDefault(this.dataType, "string"))
                .addAttribute("DEFAULTVALUE", "").addAttribute("DESCRIPTION", "").addAttribute("NAME", this.columnName)
                .addAttribute("PICTURETEXT", "").addAttribute("PORTTYPE", "INPUT/OUTPUT")
                .addAttribute("PRECISION", this.getDataPrecision()).addAttribute("SCALE", this.getDataScale());
        return qualifierField;
    }

    /**
     * *
     * 从头开始，创建一个createSourceField对象
     * @param prePhysicalLength
     * @param prePhysicalOffset
     * @param preLength
     * @param preOffset
     * @return 
     * @throws com.wzb.infa.exceptions.UnsupportedDatatypeException
     */
    public Element createSourceField(int prePhysicalLength, int prePhysicalOffset, int preLength, int preOffset)
            throws UnsupportedDatatypeException {
        String _dataType = getDataType();
        String _offset = String.valueOf(preLength + preOffset);
        String _physicalOffset = String.valueOf(prePhysicalLength + prePhysicalOffset);

        String _StringLength = this.getStringLength();
        String _physicalLength = this.getPhysicalLength();
        String _dataPrecision = this.getPhysicalLength();
        String _dataScale = this.getDataScale();

        Element sourceField = DocumentHelper.createElement("SOURCEFIELD").addAttribute("BUSINESSNAME", "")
                .addAttribute("DATATYPE", _dataType).addAttribute("DESCRIPTION", comment)
                .addAttribute("FIELDNUMBER", columnId).addAttribute("FIELDPROPERTY", "0")
                .addAttribute("FIELDTYPE", "ELEMITEM").addAttribute("HIDDEN", "NO").addAttribute("KEYTYPE", keyType)
                .addAttribute("LENGTH", _StringLength).addAttribute("LEVEL", "0").addAttribute("NAME", columnName)
                .addAttribute("NULLABLE", nullable).addAttribute("OCCURS", "0").addAttribute("OFFSET", _offset)
                .addAttribute("PHYSICALLENGTH", _physicalLength).addAttribute("PHYSICALOFFSET", _physicalOffset)
                .addAttribute("PICTURETEXT", "").addAttribute("PRECISION", _dataPrecision)
                .addAttribute("SCALE", _dataScale).addAttribute("USAGE_FLAGS", "");

        return sourceField;

    }

    /**
     *
     * 以下是构造函数、成员及其getter和setter
     */
    private String columnName;
    private String comment;
    private String dataType;
    private String dataLength;
    private String dataPrecision;
    private String dataScale;
    private String nullable;
    private String columnId;
    private String keyType;

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public String getDataType() throws UnsupportedDatatypeException {
        String _dataType;
        if (!dataTypeO2I.containsKey(this.dataType)) {
            throw new UnsupportedDatatypeException("Unsupported Data type" + columnName + ":" + this.dataType);
        } else if (this.dataType.contains("timestamp")) {

            _dataType = "timestamp";

        } else if (this.dataType.contains("number") || this.dataType.contains("float")
                || this.dataType.contains("double")) {
            _dataType = "number(p,s)";

        } else {
            _dataType = this.dataType;
        }
        return _dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public String getDataLength() {
        return dataLength;
    }

    public void setDataLength(String dataLength) {
        this.dataLength = dataLength;
    }

    public String getDataPrecision() {

        String _dataPrecision;

        if (this.dataType.contains("timestamp")) {

            _dataPrecision = "29";

        } else if (this.dataType.equals("date")) {
            _dataPrecision = "19";

        } else if (this.dataType.contains("number") || this.dataType.contains("float")
                || this.dataType.contains("double")) {

            _dataPrecision = this.getPhysicalLength();

        } else {
            _dataPrecision = this.dataLength;

        }
        return _dataPrecision;
    }

    public void setDataPrecision(String dataPrecision) {
        this.dataPrecision = dataPrecision;
    }

    public String getDataScale() {
        String _dataScale;

        if (this.dataType.contains("timestamp")) {

            _dataScale = "9";

        } else if (this.dataType.contains("number") || this.dataType.contains("float")
                || this.dataType.contains("double")) {

            _dataScale = this.dataScale;

        } else {
            _dataScale = "0";

        }
        return _dataScale;
    }

    public void setDataScale(String dataScale) {
        this.dataScale = dataScale;
    }

    public String getNullAble() {
        return nullable;
    }

    public void setNullAble(String nullAble) {
        nullable = nullAble;
    }

    public String getColumnId() {
        return columnId;
    }

    public void setColumnId(String columnId) {
        this.columnId = columnId;
    }

    public String getKeyType() {
        return keyType;
    }

    public void setKeyType(String keyType) {
        this.keyType = keyType;
    }

    public final static HashMap<String, String> dataTypeO2I = new HashMap<String, String>() {
        /**
         * 数据类型对应，将oracle的数据类型转对应成INFA的数据类型 此处写的不全，新的类型需要不断添加进来
         */
        private static final long serialVersionUID = 1L;

        {
            put("blob", "binary");
            put("clob", "string");
            put("date", "date/time");
            put("long", "text");
            put("long row", "binary");
            put("nchar", "nstring");
            put("char", "string");
            put("nclob", "ntext");
            put("number", "decimal");
            put("number(p,s)", "decimal");
            put("float", "decimal");
            put("double", "decimal");
            put("nvarchar2", "nstring");
            put("raw", "binary");
            put("timestamp", "date/time");
            put("timestamp(0)", "date/time");
            put("timestamp(3)", "date/time");
            put("timestamp(6)", "date/time");
            put("timestamp(6) with time zone", "date/time");
            put("timestamp(7)", "date/time");
            put("timestamp(9)", "date/time");
            put("timestamp(9) with time zone", "date/time");
            put("varchar", "string");
            put("varchar2", "string");
            put("xmltype", "text");
        }
    };

    public InfaCol(String columnName, String comment, String dataType, String dataLength, String dataPrecision,
            String dataScale, String nullable, String columnId, String keyType) {
        super();
        this.columnName = columnName;
        this.comment = comment;
        this.dataType = dataType.toLowerCase();
        this.dataLength = dataLength;
        this.dataPrecision = dataPrecision;
        this.dataScale = dataScale;
        this.nullable = nullable;
        this.columnId = columnId;
        this.keyType = keyType;
    }

    public String getPhysicalLength() {
        String _physicalLength;

        if (this.dataType.contains("timestamp")) {
            _physicalLength = "29";

        } else if (this.dataType.equals("date")) {
            _physicalLength = "19";

        } else if (this.dataType.contains("number") || this.dataType.contains("float")
                || this.dataType.contains("double")) {

            _physicalLength = this.dataPrecision;
            if ("0".equals(_physicalLength)) {
                _physicalLength = "22";
            }

        } else {
            _physicalLength = this.dataLength;

        }
        return _physicalLength;
    }

    public String getStringLength() {
        String _StringLength = "0";

        if (this.dataType.contains("timestamp") || this.dataType.equals("date") || this.dataType.contains("number")
                || this.dataType.contains("float") || this.dataType.contains("double")) {

            _StringLength = this.dataLength;

        }
        return _StringLength;
    }
}
