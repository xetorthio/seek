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
        DisjunctiveFormula s2 = iterator.next();
        List<ConjunctiveFormula> dnf = new ArrayList<ConjunctiveFormula>();
        for (String l1 : s1.getLiterals()) {
            for (String l2 : s2.getLiterals()) {
                dnf.add(new ConjunctiveFormula(l1, l2));
            }
        }

        while (iterator.hasNext()) {
            DisjunctiveFormula s3 = iterator.next();
            dnf = dist(dnf, s3);
        }

        return dnf;
    }

    private static List<ConjunctiveFormula> dist(List<ConjunctiveFormula> dnf,
            DisjunctiveFormula f2) {
        List<ConjunctiveFormula> ret = new ArrayList<ConjunctiveFormula>();
        for (ConjunctiveFormula f1 : dnf) {
            for (String l2 : f2.getLiterals()) {
                ConjunctiveFormula con = new ConjunctiveFormula();
                for (String l1 : f1.getLiterals())
                    con.addLiteral(l1);
                con.addLiteral(l2);
                ret.add(con);
            }
        }
        return ret;
    }
}