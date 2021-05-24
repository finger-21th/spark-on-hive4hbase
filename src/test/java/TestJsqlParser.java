import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.util.TablesNamesFinder;

import java.util.List;

public class TestJsqlParser {

    public static void main(String[] args) {
        try {
            Statement statement = CCJSqlParserUtil.parse("select * from mytable", parser -> parser.withSquareBracketQuotation(true));
            Select selectStatement = (Select) statement;
            TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
            List<String> tableList = tablesNamesFinder.getTableList(selectStatement);

            System.out.println(tableList);
        } catch (JSQLParserException e) {
            e.printStackTrace();
        }
    }
}
