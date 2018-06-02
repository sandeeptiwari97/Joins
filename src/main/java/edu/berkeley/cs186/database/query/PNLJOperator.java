package edu.berkeley.cs186.database.query;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.common.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.DataBoxException;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;

public class PNLJOperator extends JoinOperator {

  public PNLJOperator(QueryOperator leftSource,
                      QueryOperator rightSource,
                      String leftColumnName,
                      String rightColumnName,
                      Database.Transaction transaction) throws QueryPlanException, DatabaseException {
    super(leftSource,
          rightSource,
          leftColumnName,
          rightColumnName,
          transaction,
          JoinType.PNLJ);

  }


  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new PNLJIterator();
  }


  /**
   * PNLJ: Page Nested Loop Join
   *  See lecture slides.
   *
   * An implementation of Iterator that provides an iterator interface for this operator.
   *
   * Before proceeding, you should read and understand SNLJOperator.java
   *    You can find it in the same directory as this file.
   *
   * Word of advice: try to decompose the problem into distinguishable sub-problems.
   *    This means you'll probably want to add more methods than those given (Once again,
   *    SNLJOperator.java might prove to be a useful reference).
   */
  private class PNLJIterator extends JoinIterator {
    /**
     * Some member variables are provided for guidance, but there are many possible solutions.
     * You should implement the solution that's best for you, using any member variables you need.
     * You're free to use these member variables, but you're not obligated to.
     */

    private BacktrackingIterator<Page> leftIterator = null;
    private BacktrackingIterator<Page> rightIterator = null;
    private BacktrackingIterator<Record> leftRecordIterator = null;
    private BacktrackingIterator<Record> rightRecordIterator = null;
    private Record leftRecord = null;
    private Record rightRecord = null;
    private Record nextRecord = null;

    public PNLJIterator() throws QueryPlanException, DatabaseException {
      super();
      rightIterator = PNLJOperator.this.getPageIterator(this.getRightTableName());
      leftIterator = PNLJOperator.this.getPageIterator(this.getLeftTableName());
      rightIterator.next();
      leftIterator.next();

      rightRecordIterator = PNLJOperator.this.getTransaction().getBlockIterator(this.getRightTableName(), rightIterator, 1);
      leftRecordIterator = PNLJOperator.this.getTransaction().getBlockIterator(this.getLeftTableName(), leftIterator, 1);


      if (leftRecordIterator.hasNext()) {
        leftRecord = leftRecordIterator.next();
        leftRecordIterator.mark();
      } else {
        return;
      }
      if (rightRecordIterator.hasNext()) {
        rightRecord = rightRecordIterator.next();
        rightRecordIterator.mark();
      }

      try {
        fetchNextRecord();
      } catch (NoSuchElementException e) {
        nextRecord = null;
      }
    }

    /**
     * After this method is called, rightRecord will contain the first record in the rightSource.
     * There is always a first record. If there were no first records (empty rightSource)
     * then the code would not have made it this far.
     */
    private void resetRightRecord() {
      this.rightRecordIterator.reset();
      rightRecord = rightRecordIterator.next();
    }

    private void resetLeftRecord() {
      this.leftRecordIterator.reset();
      leftRecord = leftRecordIterator.next();
    }

    private void resetRightPage() throws DatabaseException {
      rightIterator = PNLJOperator.this.getPageIterator(this.getRightTableName());
      rightIterator.next();
      rightRecordIterator = PNLJOperator.this.getTransaction().getBlockIterator(this.getRightTableName(), rightIterator, 1);
      if (rightRecordIterator.hasNext()) {
        rightRecord = rightRecordIterator.next();
        rightRecordIterator.mark();
      } else {
        rightRecord = null;
      }

    }

    private void fetchNextRecord() throws DatabaseException {
      if (leftRecord == null && rightRecord == null && !leftRecordIterator.hasNext() && !rightRecordIterator.hasNext() && !leftIterator.hasNext() && !rightIterator.hasNext()) {
        nextRecord = null;
        return;
      }
      while (!(leftRecord == null && rightRecord == null && !leftRecordIterator.hasNext() && !rightRecordIterator.hasNext() && !leftIterator.hasNext() && !rightIterator.hasNext())) {
        if (leftRecord != null) {
          if (rightRecord != null) {
            DataBox leftJoinValue = leftRecord.getValues().get(PNLJOperator.this.getLeftColumnIndex());
            DataBox rightJoinValue = rightRecord.getValues().get(PNLJOperator.this.getRightColumnIndex());
            if (leftJoinValue.equals(rightJoinValue)) {
              List<DataBox> leftValues = new ArrayList<>(this.leftRecord.getValues());
              List<DataBox> rightValues = new ArrayList<>(rightRecord.getValues());
              leftValues.addAll(rightValues);
              this.nextRecord = new Record(leftValues);
              rightRecord = rightRecordIterator.hasNext() ? rightRecordIterator.next() : null;
              return;
            } else {
              rightRecord = rightRecordIterator.hasNext() ? rightRecordIterator.next() : null;
              continue;
            }
          } else {
            if (leftRecordIterator.hasNext()) {
              leftRecord = leftRecordIterator.next();
              resetRightRecord();
            } else {
              leftRecord = null;
            }
          }
        } else {
          if (rightIterator.hasNext()) {
            rightRecordIterator = PNLJOperator.this.getTransaction().getBlockIterator(this.getRightTableName(), rightIterator,  1);
            if (rightRecordIterator.hasNext()) {
              rightRecord = rightRecordIterator.next();
              rightRecordIterator.mark();
              resetLeftRecord();
            } else {
              rightRecord = null;
            }
          } else {
            if (leftIterator.hasNext()) {
              leftRecordIterator = PNLJOperator.this.getTransaction().getBlockIterator(this.getLeftTableName(), leftIterator, 1);
              if (leftRecordIterator.hasNext()) {
                leftRecord = leftRecordIterator.next();
                leftRecordIterator.mark();
                resetRightPage();
              } else {
                leftRecord = null;
              }
            } else {
              nextRecord = null;
              return;
            }
          }
        }
      }
      nextRecord = null;
    }
    /**
     * Checks if there are more record(s) to yield
     *
     * @return true if this iterator has another record to yield, otherwise false
     */
    public boolean hasNext() {
      return this.nextRecord != null;
    }

    /**
     * Yields the next record of this iterator.
     *
     * @return the next Record
     * @throws NoSuchElementException if there are no more Records to yield
     */
    public Record next() {
      if (!this.hasNext()) {
        throw new NoSuchElementException();
      }

      Record nextRecord = this.nextRecord;
      try {
        this.fetchNextRecord();
      } catch (DatabaseException e) {
        this.nextRecord = null;
      }
      return nextRecord;
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
