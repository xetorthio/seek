package redis.seek.search;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ConjunctiveFormula {
    private List<String> literals = new ArrayList<String>();

    public ConjunctiveFormula(String... literals) {
        this.literals.addAll(Arrays.asList(literals));
    }

    public void addLiteral(String literal) {
        literals.add(literal);
    }

    public List<String> getLiterals() {
        return literals;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (String literal : literals) {
            sb.append(literal);
        }
        return sb.toString();
    }
}
