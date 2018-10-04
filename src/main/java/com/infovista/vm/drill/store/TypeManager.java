package com.infovista.vm.drill.store;

import java.sql.Timestamp;
import java.util.List;
import java.util.stream.Collectors;

import javax.xml.datatype.XMLGregorianCalendar;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.vector.NullableBigIntVector;
import org.apache.drill.exec.vector.NullableFloat8Vector;
import org.apache.drill.exec.vector.NullableIntVector;
import org.apache.drill.exec.vector.NullableTimeStampVector;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.vector.ValueVector;

import com.google.common.base.Charsets;
import com.infovista.vistamart.datamodel.ws.v8.IndicatorType;
import com.infovista.vistamart.datamodel.ws.v8.MatrixDataResponseRow;
import com.infovista.vistamart.datamodel.ws.v8.PropertyType;



public abstract class TypeManager {
	protected int colIndex;
	protected ValueVector vector;

	public abstract RelDataType getRelDataType(RelDataTypeFactory typeFactory);

	public abstract MinorType getMinorType();

	public abstract void setSafe(MatrixDataResponseRow rep, int rowIndex);

	public MajorType getMajorType() {
		return org.apache.drill.common.types.Types.optional(getMinorType());
	}

	public MajorType getMajorTypeRequired() {
		return org.apache.drill.common.types.Types.required(getMinorType());
	}
	public static TypeManager getTypeManager(PropertyType type) {
		switch (type) {
		case  STRING:
		case NETADDRESS:
		case ENUM:
		case IP_6:
		case IP:
		case MAC:	
		case OID:
		case BITSTRING:
		case IPX:
		case PASSWORD:
		case CALENDAR:
			return new TypeVarchar();
		case  ID:
		case TIMETICKS:
		case UINTEGER:
		case GAUGE:
		case COUNTER_32:
			return new TypeLong();
		case TIMESTAMP:
			return new TypeTimestamp();
		case FLOAT_64:
		case COUNTER_64:
			return new TypeDouble();
		case INTEGER:
			return new TypeInteger();
		default:
			throw new UnsupportedOperationException("Unsupported type "+ type.name());
		}
	}

	public static TypeManager getTypeManager(IndicatorType type) {
		switch(type) {
		case GAUGE:
		case COUNTER_64:
		case UINTEGER:
		case COUNTER_32:
			return new TypeLong();
		case FLOAT_64:
			return new TypeDouble();
		case INTEGER:
			return new TypeInteger();
		case ENUM:
		case ALARM:
			return new TypeVarchar();

		default:
			throw new UnsupportedOperationException("Unsupported type "+ type.name());
		}
	}

	public void setValueVector(int colIndex, ValueVector vector) 
	{
		this.colIndex = colIndex;
		this.vector = vector;
	}

	public static class TypeVarchar extends TypeManager {

		@Override
		public RelDataType getRelDataType(RelDataTypeFactory typeFactory) {
			return typeFactory.createSqlType(SqlTypeName.VARCHAR);
		}

		@Override
		public MinorType getMinorType() {
			return MinorType.VARCHAR;
		}

		public void setSafe(MatrixDataResponseRow rep, int rowIndex) {
			NullableVarCharVector.Mutator mutator = (NullableVarCharVector.Mutator)vector.getMutator();
			String value = rep.getCells().get(colIndex).getSvalue();
			if(value == null){				
				List<String> stringList = rep.getCells().get(colIndex).getSvalues();
				if(stringList != null && !stringList.isEmpty()) {
					// multiValues of Type String (ex: Capabilities property)
					value = String.join(",", stringList);
				}
			}
			if(value == null) {				
				List<Long> longList = rep.getCells().get(colIndex).getLvalues();
				if(longList != null && !longList.isEmpty()) {
					//multiValues of type Long (ex: Member of Group property)
					value = longList.stream()
							.map( n -> n.toString() )
							.collect( Collectors.joining( "," ));
				}
			}
			if (value != null) {
				byte[] record = value.getBytes(Charsets.UTF_8);
				mutator.setSafe(rowIndex, record, 0, record.length);
			}
		}

	}
	public static class TypeTimestamp extends TypeManager
	{

		@Override
		public RelDataType getRelDataType(RelDataTypeFactory typeFactory) {
			return typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
		}

		@Override
		public MinorType getMinorType() {
			return MinorType.TIMESTAMP;
		}
		public void setSafe(MatrixDataResponseRow rep, int rowIndex) {
			NullableTimeStampVector.Mutator mutator = (NullableTimeStampVector.Mutator)vector.getMutator();
			XMLGregorianCalendar cd = rep.getCells().get(colIndex).getTvalue();
			Timestamp value = new Timestamp(cd.toGregorianCalendar().getTimeInMillis());
			mutator.setSafe(rowIndex, value.getTime());

		}
	}
	public static class TypeDouble extends TypeManager
	{

		@Override
		public RelDataType getRelDataType(RelDataTypeFactory typeFactory) {
			return typeFactory.createSqlType(SqlTypeName.DOUBLE);
		}

		@Override
		public MinorType getMinorType() {
			return MinorType.FLOAT8;
		}
		public void setSafe(MatrixDataResponseRow rep, int rowIndex) {
			NullableFloat8Vector.Mutator mutator = (NullableFloat8Vector.Mutator)vector.getMutator();
			Double d =rep.getCells().get(colIndex).getDvalue();
			if(d!= null)
				mutator.setSafe(rowIndex, d);	

		}

	}

	public static class TypeLong extends TypeManager
	{

		@Override
		public RelDataType getRelDataType(RelDataTypeFactory typeFactory) {
			return typeFactory.createSqlType(SqlTypeName.BIGINT);
		}

		@Override
		public MinorType getMinorType() {
			return MinorType.BIGINT;
		}
		public void setSafe(MatrixDataResponseRow rep, int rowIndex) {
			NullableBigIntVector.Mutator mutator = (NullableBigIntVector.Mutator)vector.getMutator();
			Long d =rep.getCells().get(colIndex).getLvalue();
			if(d == null) {
				//try Double
				Double doble = rep.getCells().get(colIndex).getDvalue();
				if(doble != null)
					d =  Math.round(doble);
			}
			if(d!= null)
				mutator.setSafe(rowIndex, d);	

		}

	}
	public static class TypeInteger extends TypeManager
	{

		@Override
		public RelDataType getRelDataType(RelDataTypeFactory typeFactory) {
			return typeFactory.createSqlType(SqlTypeName.INTEGER);
		}

		@Override
		public MinorType getMinorType() {
			return MinorType.INT;
		}

		public void setSafe(MatrixDataResponseRow rep, int rowIndex) {
			NullableIntVector.Mutator mutator = (NullableIntVector.Mutator)vector.getMutator();
			Integer i = rep.getCells().get(colIndex).getIvalue();
			if(i == null) {
				//try Double
				Double d = rep.getCells().get(colIndex).getDvalue();
				if(d != null)
					i = (int) Math.round(d);
			}
			if(i!=null)
				mutator.setSafe(rowIndex, i);
		}

	}
}
