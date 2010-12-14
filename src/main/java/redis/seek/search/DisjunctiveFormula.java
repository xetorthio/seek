package redis.seek.search;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DisjunctiveFormula {
    private List<String> literals = new ArrayList<String>();

    public DisjunctiveFormula(String... literals) {
        this.literals.addAll(Arrays.asList(literals));
    }

    public void addLiteral(String literal) {
        literals.add(literal);
    }

    public List<String> getLiterals() {
        return literals;
    }
}
