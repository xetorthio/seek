package redis.seek.search;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class DNF {
    public static List<ConjunctiveFormula> convert(List<DisjunctiveFormula> cnf) {
        if (cnf.size() == 1) {
            List<ConjunctiveFormula> dnf = new ArrayList<ConjunctiveFormula>();
            for (String l : cnf.get(0).getLiterals()) {
                dnf.add(new ConjunctiveFormula(l));
            }
            return dnf;
        }
        Iterator<DisjunctiveFormula> iterator = cnf.iterator();
        DisjunctiveFormula s1 = iterator.next();
        List<ConjunctiveFormula> dnf = new ArrayList<ConjunctiveFormula>();
        while (iterator.hasNext()) {
            DisjunctiveFormula s2 = iterator.next();
            for (String l1 : s1.getLiterals()) {
                for (String l2 : s2.getLiterals()) {
                    dnf.add(new ConjunctiveFormula(l1, l2));
                }
            }
        }
        return dnf;
    }
}