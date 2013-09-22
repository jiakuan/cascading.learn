/**
 * Copyright 2013 JakeCode Co., Ltd, all rights reserved.
 */
package fr.xebia.cascading.learn.level5;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/**
 * @author jiakuanwang
 */
public class SplitDocumentFunction<Context> extends BaseOperation<Context>
    implements Function<Context> {
  private static final long serialVersionUID = 1L;

  public SplitDocumentFunction(Fields docWordFields) {
    super(2, docWordFields);
  }

  @Override
  public void operate(@SuppressWarnings("rawtypes") FlowProcess flowProcess,
      FunctionCall<Context> functionCall) {
    String docId = functionCall.getArguments().getString(0);
    String docStr = functionCall.getArguments().getString(1);
    docStr = docStr.replaceAll("\\[citation needed\\]", "");

    String[] words = docStr.toLowerCase()
        .split("[\\s,\\.'\"\\d\\[\\]\\(\\)/]+");
    for (String word : words) {
      if ("v".equalsIgnoreCase(word) || "–".equalsIgnoreCase(word)) {
        continue;
      }

      functionCall.getOutputCollector().add(new Tuple(docId, word));
    }
  }
}
