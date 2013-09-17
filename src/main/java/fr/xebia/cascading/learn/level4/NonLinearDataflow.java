package fr.xebia.cascading.learn.level4;

import cascading.flow.FlowDef;
import cascading.operation.expression.ExpressionFilter;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Discard;
import cascading.tap.Tap;
import cascading.tap.hadoop.TemplateTap;
import cascading.tuple.Fields;

/**
 * Up to now, operations were stacked one after the other. But the dataflow can
 * be non linear, with multiples sources, multiples sinks, forks and merges.
 */
public class NonLinearDataflow {

  /**
   * Use {@link CoGroup} in order to know the party of each presidents.
   * You will need to create (and bind) one Pipe per source.
   * You might need to correct the schema in order to match the expected results.
   * 
   * presidentsSource field(s) : "year","president"
   * partiesSource field(s) : "year","party"
   * sink field(s) : "president","party"
   * 
   * @see http://docs.cascading.org/cascading/2.1/userguide/html/ch03s03.html#N20650
   */
  public static FlowDef cogroup(Tap<?, ?, ?> presidentsSource,
      Tap<?, ?, ?> partiesSource, Tap<?, ?, ?> sink) {
    Pipe pipe1 = new Pipe("presidentPipe");
    Pipe pipe2 = new Pipe("partyPipe");
    Pipe join = new CoGroup(pipe1, new Fields("year"), pipe2,
        new Fields("year"), new Fields("year1", "president", "year2", "party"));
    join = new Discard(join, new Fields("year1", "year2"));
    return FlowDef.flowDef().addSource(pipe1, presidentsSource)
        .addSource(pipe2, partiesSource).addTailSink(join, sink);
  }

  /**
   * Split the input in order use a different sink for each party. There is no
   * specific operator for that, use the same Pipe instance as the parent.
   * You will need to create (and bind) one named Pipe per sink.
   * 
   * source field(s) : "president","party"
   * gaullistSink field(s) : "president","party"
   * republicanSink field(s) : "president","party"
   * socialistSink field(s) : "president","party"
   * 
   * In a different context, one could use {@link TemplateTap} in order to arrive to a similar results.
   * @see http://docs.cascading.org/cascading/2.1/userguide/htmlsingle/#N214FF
   */
  public static FlowDef split(Tap<?, ?, ?> source, Tap<?, ?, ?> gaullistSink,
      Tap<?, ?, ?> republicanSink, Tap<?, ?, ?> socialistSink) {
    Pipe pipe = new Pipe("split");

    Pipe gaullistPipe = new Pipe("gaullist", pipe);
    ExpressionFilter gaullistFilter = new ExpressionFilter(
        "!party.equalsIgnoreCase(\"Gaullist\")", String.class);
    gaullistPipe = new Each(gaullistPipe, new Fields("party"), gaullistFilter);

    Pipe republicanPipe = new Pipe("republican", pipe);
    ExpressionFilter republicanFilter = new ExpressionFilter(
        "!party.equalsIgnoreCase(\"Republican\")", String.class);
    republicanPipe = new Each(republicanPipe, new Fields("party"),
        republicanFilter);

    Pipe socialistPipe = new Pipe("socialist", pipe);
    ExpressionFilter socialistFilter = new ExpressionFilter(
        "!party.equalsIgnoreCase(\"Socialist\")", String.class);
    socialistPipe = new Each(socialistPipe, new Fields("party"),
        socialistFilter);

    return FlowDef.flowDef().addSource(pipe, source)
        .addTailSink(gaullistPipe, gaullistSink)
        .addTailSink(republicanPipe, republicanSink)
        .addTailSink(socialistPipe, socialistSink);
  }
}
