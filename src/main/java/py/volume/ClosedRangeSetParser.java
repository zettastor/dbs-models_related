/*
 * Copyright (c) 2022-2022. PengYunNetWork
 *
 * This program is free software: you can use, redistribute, and/or modify it
 * under the terms of the GNU Affero General Public License, version 3 or later ("AGPL"),
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *
 *  You should have received a copy of the GNU Affero General Public License along with
 *  this program. If not, see <http://www.gnu.org/licenses/>.
 */

package py.volume;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import java.util.Stack;
import org.apache.commons.lang.Validate;

/**
 * Convert from String to {@code com.google.common.collect.RangeSet}.currently only closed range set
 * supported.
 *
 */
public class ClosedRangeSetParser {
  private static final char PRE_SQUARE_BRACKET = '[';
  private static final char POS_SQUARE_BRACKET = ']';
  private static final char BETWEEN_NUM = '.';
  private static final char COMMA = ',';
  private static final char BLANK = ' ';

  public static RangeSet<Integer> parseRange(String rangeString) throws NumberFormatException {
    RangeSet<Integer> rangeSet = TreeRangeSet.create();
    if (rangeString == null) {
      return null;
    }
    int length = rangeString.length();

    Stack<String> charStack = new Stack<String>();
    int lowerBound = 0;
    int upperBound = 0;
    for (int i = 0; i < length; i++) {
      char ch = rangeString.charAt(i);
      // logger.debug("index {}, char {}, stack {}", i, ch, charStack);
      switch (ch) {
        case BLANK:
          break;
        case COMMA:
          break;
        case BETWEEN_NUM:
          charStack.push(String.valueOf(ch));
          break;
        case PRE_SQUARE_BRACKET:
          charStack.push(String.valueOf(ch));
          break;
        case POS_SQUARE_BRACKET:
          char chPoped = charStack.pop().charAt(0);
          if (chPoped == PRE_SQUARE_BRACKET) {
            break;
          }
          int pow = 0;
          while (chPoped != BETWEEN_NUM) {
            int num = Integer.parseInt(String.valueOf(chPoped));
            upperBound += num * Math.pow(10, pow);
            pow++;
            chPoped = charStack.pop().charAt(0);
          }
          Validate.isTrue(chPoped == BETWEEN_NUM);

          //two "."
          while (chPoped == BETWEEN_NUM) {
            chPoped = charStack.pop().charAt(0);
          }
          //  Validate.isTrue(chPoped == BETWEEN_NUM);

          pow = 0;
          while (chPoped != PRE_SQUARE_BRACKET) {
            int num = Integer.parseInt(String.valueOf(chPoped));
            lowerBound += num * Math.pow(10, pow);
            pow++;
            chPoped = charStack.pop().charAt(0);
          }
          Validate.isTrue(lowerBound <= upperBound);
          // logger.debug("add range {}, {}", lowerBound, upperBound);
          rangeSet.add(Range.closed(lowerBound, upperBound));
          lowerBound = 0;
          upperBound = 0;
          break;
        default:
          try {
            charStack.push(String.valueOf(ch));
          } catch (NumberFormatException e) {
            throw e;
          }
      }
    }

    return rangeSet;
  }
}