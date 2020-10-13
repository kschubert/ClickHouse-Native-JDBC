package com.github.housepower.jdbc.data.type.complex;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Locale;

import com.github.housepower.jdbc.connect.PhysicalInfo;
import com.github.housepower.jdbc.data.IDataType;
import com.github.housepower.jdbc.misc.SQLLexer;
import com.github.housepower.jdbc.misc.StringView;
import com.github.housepower.jdbc.misc.Validate;
import com.github.housepower.jdbc.serializer.BinaryDeserializer;
import com.github.housepower.jdbc.serializer.BinarySerializer;

public class DataTypeDecimal implements IDataType {

    private final String name;

    private final int precision;

    private final int scale;

    private final BigDecimal scaleFactor;

    private final int nobits;

    public DataTypeDecimal(String name, int precision, int scale) {
        this.name = name;
        this.precision = precision;
        this.scale = scale;
        this.scaleFactor = BigDecimal.valueOf(Math.pow(10, scale));
        if (this.precision <= 9) {
            this.nobits = 32;
        } else if (this.precision <= 18) {
            this.nobits = 64;
        } else if (this.precision <= 38) {
            this.nobits = 128;
        } else if (this.precision <= 76) {
            this.nobits = 256;
        } else {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH, "Precision[%d] is out of boundary.", precision));
        }
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public int sqlTypeId() {
        return Types.DECIMAL;
    }

    @Override
    public Object defaultValue() {
        return new BigDecimal(0);
    }

    @Override
    public Class javaTypeClass() {
        return BigDecimal.class;
    }

    @Override
    public boolean nullable() {
        return false;
    }

    @Override
    public Object deserializeTextQuoted(SQLLexer lexer) throws SQLException {
        BigDecimal result = null;
        if (lexer.isCharacter('\'')) {
            StringView v = lexer.stringLiteral();
            result = new BigDecimal(v.toString());
        } else {
            Number v = lexer.numberLiteral();
            result = BigDecimal.valueOf(v.doubleValue());
        }
        result = result.setScale(scale, RoundingMode.HALF_UP);
        return result;
    }

    @Override
    public void serializeBinary(Object data, BinarySerializer serializer) throws IOException {
        BigDecimal targetValue;
        if (data instanceof BigInteger) {
            targetValue = new BigDecimal((BigInteger) data);
        } else if (data instanceof BigDecimal) {
            targetValue = ((BigDecimal) data).multiply(scaleFactor);
        } else {
            throw new IllegalArgumentException("value must be one of BigInteger/BigDecimal, actual class is " + data.getClass().getCanonicalName());
        }
        int targetBytes = this.nobits / 8;
        BigInteger plainIntegerValue = targetValue.toBigInteger();
        byte[] byteArray = plainIntegerValue.toByteArray();
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        int bytesToBeAdded = targetBytes - byteArray.length;
        while (bytesToBeAdded-- > 0) {
            buffer.write(byteArray[0] < 0 ? 0xff : 0x00);
        }
        for (byte aByte : byteArray) {
            if (buffer.size() == targetBytes) {
                break;
            }
            buffer.write(aByte);
        }
        byte[] targetArray = buffer.toByteArray();
        for (int i = targetArray.length; i > 0; i--) {
            serializer.writeByte(targetArray[i - 1]);
        }
    }

    @Override
    public Object deserializeBinary(BinaryDeserializer deserializer) throws SQLException, IOException {
        byte[] v = new byte[this.nobits/8];
        for (int i = v.length; i > 0; i--) {
            v[i-1] = deserializer.readByte();
        }
        BigDecimal value = new BigDecimal(new BigInteger(v)).divide(scaleFactor);
        value = value.setScale(scale, RoundingMode.HALF_UP);
        return value;
    }

    @Override
    public Object[] deserializeBinaryBulk(int rows, BinaryDeserializer deserializer) throws SQLException, IOException {
        BigDecimal[] data = new BigDecimal[rows];
        for (int row = 0; row < rows; row++) {
            data[row] = (BigDecimal) this.deserializeBinary(deserializer);
        }
        return data;
    }

    public static IDataType createDecimalType(SQLLexer lexer, PhysicalInfo.ServerInfo serverInfo) throws SQLException {
        Validate.isTrue(lexer.character() == '(');
        Number precision = lexer.numberLiteral();
        Validate.isTrue(lexer.character() == ',');
        Number scale = lexer.numberLiteral();
        Validate.isTrue(lexer.character() == ')');
        return new DataTypeDecimal("Decimal(" + precision.intValue() + "," + scale.intValue() + ")", precision.intValue(), scale.intValue());
    }

    @Override
    public int precisionOrLength() {
        return precision;
    }

    @Override
    public int scale() {
        return scale;
    }
}
