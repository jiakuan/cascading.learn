/**
 * Copyright 2013 JakeCode Co., Ltd, all rights reserved.
 */
package fr.xebia.cascading.learn.level5;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.slf4j.LoggerFactory;

/**
 * Utility class for dealing with regular expression matching
 * @author jiakuanwang
 */
public class RegExUtil {
  public static int indexOf(String text, String regex, boolean ignoreCase) {
    Pattern pattern = Pattern.compile(regex);
    if (ignoreCase) {
      pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
    }

    Matcher match = pattern.matcher(text);
    if (match.find()) {
      return match.start();
    }
    return -1;
  }

  public static boolean isMatch(final RegExResult result, String regex,
      String text) {
    return check(result, regex, false, text).isMatched();
  }

  public static boolean isMatchIgnoreCase(final RegExResult result,
      String regex, String text) {
    return check(result, regex, true, text).isMatched();
  }

  public static RegExResult check(String regex, String text) {
    return check(null, regex, false, text);
  }

  public static RegExResult checkIgnoreCase(String regex, String text) {
    return check(regex, true, text);
  }

  private static RegExResult check(String regex, boolean ignoreCase, String text) {
    return check(null, regex, ignoreCase, text);
  }

  private static RegExResult check(RegExResult result, String regex,
      boolean ignoreCase, String text) {
    if (result == null) {
      result = new RegExResult();
    }
    result.clear();
    if (regex == null) {
      LoggerFactory.getLogger(RegExUtil.class).info("PatternStr was null");
      return result;
    }
    result.regexing = regex;

    if (text == null) {
      LoggerFactory.getLogger(RegExUtil.class).info("text was null, no match");
      return result;
    }

    try {
      Pattern pattern = Pattern.compile(regex);
      if (ignoreCase) {
        pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
      }

      Matcher match = pattern.matcher(text);

      boolean matched = false;
      while (match.find()) {
        matched = true;

        RegExMatch regExMatch = new RegExMatch();
        regExMatch.wholeMatch = match.group();
        for (int i = 1; i <= match.groupCount(); i++) {
          regExMatch.groupItems.add(match.group(i));
        }
        result.matches.add(regExMatch);
      }

      result.setMatched(matched);
      return result;
    } catch (Exception e) {
      throw new RegExException(String.format("Patt %s , text %s, error %s",
          regex, text, e.getMessage()));
    }
  }

  public static int countOccurences(String regex, String text) {
    int count = 0;
    try {
      Pattern pattern = Pattern.compile(regex);
      Matcher match = pattern.matcher(text);
      while (match.find()) {
        count++;
      }
    } catch (Exception e) {
      throw new RegExException(String.format("Patt %s , text %s, error %s",
          regex, text, e.getMessage()));
    }
    return count;
  }

  public static class RegExMatch {
    private final List<String> groupItems = new ArrayList<String>();
    private String wholeMatch;

    public String get(int groupNum) {
      if (groupNum > groupItems.size()) {
        throw new RegExException(String.format(
            "Group num %d is higher than result size %d", groupNum,
            groupItems.size()));
      }
      return groupItems.get(groupNum - 1);
    }

    public int getNumOfGroups() {
      return groupItems.size();
    }

    public String getMatched() {
      if (wholeMatch == null) {
        throw new RegExException("wholeMatch null");
      }
      return wholeMatch;
    }
  }

  public static class RegExResult {
    private boolean isMatched;
    private List<RegExMatch> matches = new ArrayList<RegExMatch>();
    private String regexing;

    public String get(int groupNum) {
      if (matches.size() > 0) {
        return matches.get(0).get(groupNum);
      } else {
        throw new RegExException(String.format(
            "Group num %d cannot be handled as no matches found", groupNum));
      }
    }

    public int getNumOfGroups() {
      if (matches.size() > 0) {
        return matches.get(0).getNumOfGroups();
      } else {
        throw new RegExException(
            String.format("Cannot get num of groups as no matches found"));
      }
    }

    public String getMatched() {
      if (matches.size() > 0) {
        return matches.get(0).getMatched();
      } else {
        throw new RegExException(
            String.format("Cannot matched string as no matches found"));
      }
    }

    public void clear() {
      regexing = StringUtils.EMPTY;
      matches.clear();
    }

    /**
     * @param isMatched the isMatched to set
     */
    public void setMatched(boolean isMatched) {
      this.isMatched = isMatched;
    }

    /**
     * @return the isMatched
     */
    public boolean isMatched() {
      return isMatched;
    }

    public String getPatternString() {
      return regexing;
    }

    /**
     * @return the matches
     */
    public List<RegExMatch> getMatches() {
      return matches;
    }
  }

  public static class RegExException extends RuntimeException {
    public RegExException(String message) {
      super(message);
    }
  }
}
