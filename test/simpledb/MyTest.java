package simpledb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import junit.framework.JUnit4TestAdapter;

import org.junit.Before;
import org.junit.Test;

import simpledb.common.Utility;
import simpledb.execution.Filter;
import simpledb.execution.OpIterator;
import simpledb.execution.Predicate;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.systemtest.SimpleDbTestBase;

public class MyTest extends SimpleDbTestBase{

    final int testWidth = 3;
    OpIterator scan;


    @Before public void setUp() {
        this.scan = new TestUtil.MockScan(-5, 5, testWidth);
      }
    /**
   * Unit test for Filter.getNext() using a &lt; predicate that filters
   *   some tuples
   */
    @Test
    public void filterSomeLessThan() throws Exception {
        Predicate pred;
        pred = new Predicate(0, Predicate.Op.LESS_THAN, TestUtil.getField(2));
        Filter op = new Filter(pred, scan);
        TestUtil.MockScan expectedOut = new TestUtil.MockScan(-5, 2, testWidth);
        // while (expectedOut.hasNext()) {
        //     Tuple expected = expectedOut.next();
        //     System.out.println(expected);
        // }
        op.open();
        TestUtil.compareDbIterators(op, expectedOut);
        op.close();
    }
  
   /**
   * JUnit suite target
   */
  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(MyTest.class);
  }
}
