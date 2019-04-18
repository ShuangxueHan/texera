package com.google.re2j;

import edu.uci.ics.texera.dataflow.regexmatcher.SubRegex;
import org.apache.commons.lang3.StringEscapeUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.List;

/**
 * Public Wrapper class for re2j.Regexp.<!-- --> This class represents the
 * abstract syntax tree. <br>
 * 
 * <p>
 * For example, <br>
 * regex: "abc", abstract syntax tree:<br>
 * CONCAT <br>
 * --LITERAL a <br>
 * --LITERAL b <br>
 * --LITERAL c <br>
 * </p>
 * 
 * <p>
 * regex: "a*|b", abstract syntax tree: <br>
 * ALTERNATE <br>
 * --STAR <br>
 * ----LITERAL a <br>
 * --LITERAL b <br>
 * </p>
 * 
 * <p>
 * regex: "[a-f]{1-3}", abstract syntax tree: <br>
 * REPEAT min:1, max:3 <br>
 * --CHAR_CLASS a-f <br>
 * </p>
 * 
 * @author Zuozhi Wang
 *
 */
public class PublicRegexp extends Regexp {
    /*
     * // Fields originally declared in Regexp. // For detailed explanations
     * please see cooresponding getter methods
     * 
     * // Note that for subexpressions, although original comments // say it's
     * never null, it could still be null.
     * 
     * Op op; // operator int flags; // bitmap of parse flags Regexp[] subs; //
     * subexpressions, if any. Never null. // subs[0] is used as the freelist.
     * 
     * int[] runes; // matched runes, for LITERAL, CHAR_CLASS int min, max; //
     * min, max for REPEAT int cap; // capturing index, for CAPTURE String name;
     * // capturing name, for CAPTURE
     * 
     */

    // publicSubs are an array of subexpressions with type PublicRegexp
    PublicRegexp[] publicSubs;

    /**
     * This calls the shallow copy constructor in Regexp superclass, which only
     * copies reference to subexpressions array. <br>
     */
    private PublicRegexp(Regexp that) {
        super(that);
    }

    /**
     * This performs a deep copy of a Regexp object. Every Regexp Object in
     * subexpression arrary is converted to a PublicRegexp object and put in
     * publicSubs array. <br>
     * This is the only public entry point to construct a PublicRegexp object.
     * <br>
     * 
     * @param re,
     *            a Regexp that needs to be converted to PublicRegexp
     * @return PublicRegexp
     */
    public static PublicRegexp deepCopy(Regexp re) {
        PublicRegexp publicRegexp = new PublicRegexp(re);
        if (re.subs != null) {
            // initialize publicSubs array
            publicRegexp.publicSubs = new PublicRegexp[re.subs.length];
            // map every Regexp sub-expression to a PublicRegexp sub-expression
            Stream<PublicRegexp> publicSubStream = Arrays.asList(re.subs).stream()
                    .map(sub -> PublicRegexp.deepCopy(sub));
            // convert the result PublicRegexp subexpressions to an array
            publicSubStream.collect(Collectors.toList()).toArray(publicRegexp.publicSubs);
        } else {
            publicRegexp.publicSubs = null;
        }
        return publicRegexp;
    }

    /**
     * Enum types of Op (operator), which represents the operator type of
     * current node in abstract syntax tree. <br>
     * This enum is identical to Regex.Op, which is not public. <br>
     * 
     * @author zuozhi
     *
     */
    public enum PublicOp {
        NO_MATCH, // Matches no strings.
        EMPTY_MATCH, // Matches empty string.
        LITERAL, // Matches runes[] sequence
        CHAR_CLASS, // Matches Runes interpreted as range pair list
        ANY_CHAR_NOT_NL, // Matches any character except '\n'
        ANY_CHAR, // Matches any character
        BEGIN_LINE, // Matches empty string at end of line
        END_LINE, // Matches empty string at end of line
        BEGIN_TEXT, // Matches empty string at beginning of text
        END_TEXT, // Matches empty string at end of text
        WORD_BOUNDARY, // Matches word boundary `\b`
        NO_WORD_BOUNDARY, // Matches word non-boundary `\B`
        CAPTURE, // Capturing subexpr with index cap, optional name name
        STAR, // Matches subs[0] zero or more times.
        PLUS, // Matches subs[0] one or more times.
        QUEST, // Matches subs[0] zero or one times.
        REPEAT, // Matches subs[0] [min, max] times; max=-1 => no limit.
        CONCAT, // Matches concatenation of subs[]
        ALTERNATE, // Matches union of subs[]

        // Pseudo ops, used internally by Parser for parsing stack:
        // These shouldn't be in the final parse tree
        LEFT_PAREN, VERTICAL_BAR;
    }

    /**
     * This returns the op's type, {@link PublicOp}, which is equivalent to
     * Regexp.Op. <br>
     * 
     * @return PublicRegex.PublicOp, an enum type representing the operator
     */
    public PublicOp getOp() {
        try {
            return PublicOp.valueOf(this.op.toString());
        } catch (IllegalArgumentException e) {
            return PublicOp.NO_MATCH;
        }

    }

    /**
     * This returns a bitmap of parse flags. <br>
     * 
     * @see PublicRE2 for possible flags
     * @return a bitmap of parse flags
     */
    public int getFlags() {
        return this.flags;
    }

    /**
     * This returns an array of sub-expressions with type PublicRegexp. <br>
     * 
     * @return an array of subexpressions
     */
    public PublicRegexp[] getSubs() {
        return this.publicSubs;
    }

    /**
     * Runes are a sequence of characters. It stores information related to
     * literals and character classes, and has different interpretations for
     * different ops. <br>
     * <p>
     * For example, <br>
     * regex: "[a-z]", runes: [a,z] <br>
     * interpretation: a character class from a to z <br>
     * regex: "[a-cx-z]", runes: [a,c,x,z] <br>
     * interpretation: a character class from a to c, and from x to z <br>
     * regex: "cat", runes [c,a,t] <br>
     * interpretation: a literal "cat" <br>
     * </p>
     * 
     * @return an array of runes
     */
    public int[] getRunes() {
        return this.runes;
    }

    /**
     * Min and Max are used for repetitions numbers. <br>
     * <p>
     * For example, <br>
     * regex: "a{3,5}", min will be 3, and max will be 5 <br>
     * </p>
     * 
     * @return an int indicating minimum number of repetitions
     */
    public int getMin() {
        return this.min;
    }

    /**
     * @see //getMin
     * @return an int indicating maxinum number of repetitions
     */
    public int getMax() {
        return this.max;
    }

    /**
     * Cap is the capturing index. Expressions in () become a capture group. The
     * entire regex's capturing index is 0, and other groups' indexes start from
     * 1. <br>
     * <p>
     * For example, <br>
     * regex: "(a)(b)" <br>
     * for "(a)", cap will be 1, for "(b)", cap will be 2 <br>
     * </p>
     * 
     * @return an int indicating capture index
     */
    public int getCap() {
        return this.cap;
    }

    /**
     * Name is capturing group's name (if any). <br>
     * <p>
     * For example, <br>
     * regex: {@literal "(?<name1>a)(?\<name2>b)"} <br>
     * for {@literal "(?\<name1>a)"}, cap name will be name1 <br>
     * for {@literal "(?\<name2>b)"}, cap name will be name2 <br>
     * </p>
     * 
     * @return an int indicating capture index
     */
    public String getCapName() {
        return this.name;
    }
    public List<String> getSequence(PublicRegexp re){
        List<String> sequences = new ArrayList<>();
        // System.out.println(re.toString()+" "+ re.getOp().toString());

        if(re.getOp() == PublicRegexp.PublicOp.CONCAT){
            for (PublicRegexp sub : re.getSubs()) {
                if (sub.getOp() == PublicOp.CHAR_CLASS)
                    sequences.add(sub.name);
                else
                   sequences = getSequence(sub);
            }

        }else if(re.getOp() == PublicOp.ALTERNATE){
            for(PublicRegexp sub : re.getSubs()) {
                //System.out.println(" " + sub.toString());
               sequences.addAll(getSequence(sub));
            }

        }else if(re.getOp() == PublicOp.QUEST){
            // minLength++;

        }else if(re.getOp() == PublicOp.CHAR_CLASS){
            sequences.add(re.name);

        }else if(re.getOp() == PublicOp.CAPTURE){
            PublicRegexp sub1 = re.getSubs()[0];
            sequences.addAll(getSequence(sub1));


        }else if(re.getOp() == PublicOp.REPEAT){
            //System.out.println(re.toString()+" repeat "+ re.getOp().toString());

        }else if(re.getOp() == PublicOp.STAR || re.getOp() == PublicOp.PLUS){
            // System.out.println("====== "+re.toString().charAt(0));

            PublicRegexp sub1 = re.getSubs()[0];
            sequences.addAll(getSequence(sub1));

        }else {
            //System.out.println("re" + StringEscapeUtils.unescapeJava(re.toString()));
            sequences.add(re.toString());
        }
        return sequences;

    }
    //TODO hasOp
    public static boolean hasOp(PublicRegexp re, PublicOp op){
       //  System.out.println(re.toString()+ "--------"+re.getOp()+ " "+ op);
        if (re.getOp() == op)
            return true;
        else {
            if(re.getOp() == PublicOp.LITERAL || re.getOp() == PublicOp.CHAR_CLASS) return false;
            for (PublicRegexp sub : re.getSubs()) {
                if (hasOp(sub, op)) {
                    return true;
                }
                else return false;
            }
            return false;
        }
    }

    public static String reverseDeepCopy(PublicRegexp sub){
        //System.out.println("reverseDeepCopy " + sub.toString());
        //PublicRegexp publicRegexp = new PublicRegexp(re);
        //StringBuffer reverseSubStringBuffer = new StringBuffer();
        String reverseSubString = "";
        if (sub != null) {
           // System.out.println("reverseOp " + sub.getOp().toString());
            // map every Regexp sub-expression to a PublicRegexp sub-expression
            //Stream<PublicRegexp> publicSubStream = Arrays.asList(sub).stream().map(sub -> PublicRegexp.deepCopy(sub));
            // reverse sub.toString()
            if(sub.getOp() == PublicOp.ALTERNATE) {

                for (PublicRegexp sub1 : sub.getSubs()) {

                   String subString = reverseDeepCopy(sub1);
                   reverseSubString += subString;

                   reverseSubString += "|";


                }
                reverseSubString = reverseSubString.substring(0, reverseSubString.length()-1);
                reverseSubString = "(" + reverseSubString + ")";
                //System.out.println("Copy " + reverseSubString);

            }else if(sub.getOp() == PublicOp.CONCAT || sub.getOp() == PublicOp.CAPTURE || sub.getOp() == PublicOp.STAR
                    || sub.getOp() == PublicOp.QUEST || sub.getOp() == PublicOp.PLUS){
                for(int i = sub.publicSubs.length-1; i >= 0; i--){
                    PublicRegexp sub2  = sub.publicSubs[i];
                    //if()
                     reverseSubString += reverseDeepCopy(sub2);

                }
                if(sub.getOp() == PublicOp.CAPTURE) {

                    reverseSubString = "(" + reverseSubString + ")";
                    //System.out.println("reverseString"+ reverseSubString);
                }
                else if(sub.getOp() == PublicOp.PLUS){
                    reverseSubString = reverseSubString + "+";
                }else if(sub.getOp() == PublicOp.STAR){
                    reverseSubString = reverseSubString + "*";
                }else if(sub.getOp() == PublicOp.QUEST){
                    reverseSubString = reverseSubString + "?";
                }
            }else if (sub.getOp() == PublicOp.CHAR_CLASS){
                reverseSubString = sub.toString();

            } else if(sub.getOp() == PublicOp.LITERAL){
                if(sub.toString().startsWith("\\"))
                    reverseSubString = sub.toString();
                else {
                    String subString = sub.toString().replaceAll("\\\\.", ".");
                    StringBuffer reversesub = new StringBuffer(subString);
                    reversesub.reverse();

                    //System.out.println("reversesub " + reversesub.toString());
                    reverseSubString = reversesub.toString();
                }
            }else reverseSubString = sub.toString();

        }else
            return null;
        return reverseSubString;
    }

    public static int computeMinLength(PublicRegexp re){
        int minLength = 0;
       // System.out.println(re.toString()+" "+ re.getOp().toString());

        if(re.getOp() == PublicRegexp.PublicOp.CONCAT){
            for (PublicRegexp sub : re.getSubs()) {
                if (sub.getOp() == PublicOp.CHAR_CLASS)
                    minLength++;
                else
                    minLength += computeMinLength(sub);
            }

        }else if(re.getOp() == PublicOp.ALTERNATE){
            minLength = computeMinLength(re.getSubs()[0]);
            for(PublicRegexp sub : re.getSubs()) {
                //System.out.println(sub.toString() + "====== "+re.toString().charAt(0));
                if(computeMinLength(sub)<minLength)
                    minLength = computeMinLength(sub);
                else continue;
            }

        }else if(re.getOp() == PublicOp.QUEST){
            //minLength++;

        }else if(re.getOp() == PublicOp.CHAR_CLASS){
             minLength++;

        }else if(re.getOp() == PublicOp.CAPTURE){
            PublicRegexp sub1 = re.getSubs()[0];
            minLength = computeMinLength(sub1);


        }else if(re.getOp() == PublicOp.REPEAT){
            //System.out.println(re.toString()+" repeat "+ re.getOp().toString());
            minLength = re.getMin();
        }else if(re.getOp() == PublicOp.LITERAL){
           // System.out.println("====== "+re.toString().charAt(0));
            // for regex \\.
            int count = 0;
            for(int i=0; i< re.toString().length(); i++) {
                if (re.toString().charAt(i) == '\\')
                    count++;
            }
            minLength = re.toString().length()-count;

        }
        return minLength;
    }

    public static int computeMaxLength(PublicRegexp re){
        int maxLength = 0;
        // System.out.println(re.toString()+" "+ re.getOp().toString());

        if(re.getOp() == PublicRegexp.PublicOp.CONCAT){
            for (PublicRegexp sub : re.getSubs()) {
                if (sub.getOp() == PublicOp.CHAR_CLASS)
                    maxLength++;
                else
                    maxLength += computeMaxLength(sub);
            }

        }else if(re.getOp() == PublicOp.ALTERNATE){
            maxLength = computeMaxLength(re.getSubs()[0]);
            for(PublicRegexp sub : re.getSubs()) {
                //System.out.println(sub.toString() + "====== "+re.toString().charAt(0));
                if(computeMaxLength(sub)>maxLength)
                    maxLength = computeMaxLength(sub);
                else continue;
            }

        }else if(re.getOp() == PublicOp.QUEST){
            maxLength++;

        }else if(re.getOp() == PublicOp.CHAR_CLASS){
            maxLength++;

        }else if(re.getOp() == PublicOp.CAPTURE){
            PublicRegexp sub1 = re.getSubs()[0];
            maxLength = computeMaxLength(sub1);


        }else if(re.getOp() == PublicOp.REPEAT){
            //System.out.println(re.toString()+" repeat "+ re.getOp().toString());
            maxLength = re.getMax();
        }else if(re.getOp() == PublicOp.LITERAL){
            // System.out.println("====== "+re.toString().charAt(0));
            // for regex \\.
            int count = 0;
            for(int i=0; i< re.toString().length(); i++) {
                if (re.toString().charAt(i) == '\\')
                    count++;
            }
            maxLength = re.toString().length()-count;

        }
        return maxLength;
    }
}
