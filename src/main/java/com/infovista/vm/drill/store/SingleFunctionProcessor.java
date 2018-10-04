package com.infovista.vm.drill.store;

import org.apache.drill.common.expression.CastExpression;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.ValueExpressions.BooleanExpression;
import org.apache.drill.common.expression.ValueExpressions.DateExpression;
import org.apache.drill.common.expression.ValueExpressions.DoubleExpression;
import org.apache.drill.common.expression.ValueExpressions.FloatExpression;
import org.apache.drill.common.expression.ValueExpressions.IntExpression;
import org.apache.drill.common.expression.ValueExpressions.LongExpression;
import org.apache.drill.common.expression.ValueExpressions.QuotedString;
import org.apache.drill.common.expression.ValueExpressions.TimeExpression;
import org.apache.drill.common.expression.ValueExpressions.TimeStampExpression;
import org.apache.drill.common.expression.ValueExpressions.VarDecimalExpression;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

/**
 * Process a single function/comparison, i.e. a = '1' into the component parts.
 */
public final class SingleFunctionProcessor extends
AbstractExprVisitor<Boolean, LogicalExpression, RuntimeException>{

  private boolean success;
  private Object value;
  private SchemaPath path;
  private String functionName;

  public static boolean isCompareFunction(String functionName) {
    return COMPARE_FUNCTIONS_TRANSPOSE_MAP.keySet().contains(functionName);
  }

  public static SingleFunctionProcessor process(FunctionCall call) {
    String functionName = call.getName();
    functionName = functionName.toLowerCase().replace(" ", "");

    LogicalExpression nameArg = call.args.get(0);
    LogicalExpression valueArg = call.args.size() >= 2 ? call.args.get(1) : null;
    SingleFunctionProcessor evaluator = new SingleFunctionProcessor(functionName);
    if (valueArg != null) { // binary function
        if (VALUE_EXPRESSION_CLASSES.contains(nameArg.getClass())) {
          LogicalExpression swapArg = valueArg;
          valueArg = nameArg;
          nameArg = swapArg;
          evaluator.functionName = COMPARE_FUNCTIONS_TRANSPOSE_MAP
              .get(functionName);
        }
        evaluator.success = nameArg.accept(evaluator, valueArg);
    } else if (call.args.get(0) instanceof SchemaPath) {
      evaluator.success = true;
      evaluator.path = (SchemaPath) nameArg;
    }

    return evaluator;
  }

  private  SingleFunctionProcessor(String functionName) {
    this.success = false;
    this.functionName = functionName;
  }

  public Object getValue() {
    return value;
  }

  public boolean isSuccess() {
    return success;
  }

  public SchemaPath getPath() {
    return path;
  }

  public String getFunctionName() {
    return functionName;
  }

  @Override
  public Boolean visitCastExpression(CastExpression e,
      LogicalExpression valueArg) throws RuntimeException {
    if (e.getInput() instanceof CastExpression
        || e.getInput() instanceof SchemaPath) {
      return e.getInput().accept(this, valueArg);
    }
    return false;
  }
  
  @Override
  public Boolean visitUnknown(LogicalExpression e, LogicalExpression valueArg)
      throws RuntimeException {
    return false;
  }

  @Override
  public Boolean visitSchemaPath(SchemaPath path, LogicalExpression valueArg)
      throws RuntimeException {
    if (valueArg instanceof QuotedString) {
      this.value = ((QuotedString) valueArg).value;
      this.path = path;
      return true;
    }

    if (valueArg instanceof LongExpression) {
        this.value = ((LongExpression) valueArg).getLong();
        this.path = path;
        return true;
      }
    
    if (valueArg instanceof IntExpression) {
      this.value = ((IntExpression) valueArg).getInt();
      this.path = path;
      return true;
    }

   

    if (valueArg instanceof FloatExpression) {
      this.value = ((FloatExpression) valueArg).getFloat();
      this.path = path;
      return true;
    }

    if (valueArg instanceof DoubleExpression) {
      this.value = ((DoubleExpression) valueArg).getDouble();
      this.path = path;
      return true;
    }

    if (valueArg instanceof BooleanExpression) {
      this.value = ((BooleanExpression) valueArg).getBoolean();
      this.path = path;
      return true;
    }

    if (valueArg instanceof VarDecimalExpression) {
      this.value = ((VarDecimalExpression) valueArg).getBigDecimal().doubleValue();
      this.path = path;
      return true;
    }
    
    if(valueArg instanceof TimeStampExpression) {
    	this.value = ((TimeStampExpression)valueArg).getTimeStamp();
    	this.path = path;
    	return true;
    }
   

    return false;
  }
  
  private static final ImmutableSet<Class<? extends LogicalExpression>> VALUE_EXPRESSION_CLASSES;
  static {
    ImmutableSet.Builder<Class<? extends LogicalExpression>> builder = ImmutableSet
        .builder();
    VALUE_EXPRESSION_CLASSES = builder.add(BooleanExpression.class)
        .add(DateExpression.class).add(DoubleExpression.class)
        .add(FloatExpression.class).add(IntExpression.class)
        .add(LongExpression.class).add(QuotedString.class)
        .add(TimeExpression.class).add(VarDecimalExpression.class).add(TimeStampExpression.class).build();
  }

  private static final ImmutableMap<String, String> COMPARE_FUNCTIONS_TRANSPOSE_MAP;

  static {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    COMPARE_FUNCTIONS_TRANSPOSE_MAP = builder
      // unary functions
      .put("isnotnull", "isnotnull")
      .put("isNotNull", "isNotNull")
      .put("is not null", "is not null")
      .put("isnull", "isnull")
      .put("isNull", "isNull")
      .put("is null", "is null")
      // binary functions
      .put("like", "like")
      .put("equal", "equal")
      .put("not_equal", "not_equal")
      .put("greater_than_or_equal_to", "less_than_or_equal_to")
      .put("greater_than", "less_than")
      .put("less_than_or_equal_to", "greater_than_or_equal_to")
      .put("less_than", "greater_than")
      .build();
  }
}