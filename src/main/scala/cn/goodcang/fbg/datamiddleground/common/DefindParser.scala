package cn.goodcang.fbg.datamiddleground.common

import net.sf.jsqlparser.expression.Expression
import net.sf.jsqlparser.parser.{CCJSqlParser, CCJSqlParserUtil}
import net.sf.jsqlparser.statement.Statement
import net.sf.jsqlparser.statement.select._
import net.sf.jsqlparser.util.TablesNamesFinder
import java.util
import java.util.function.Consumer


object DefindParser {

  def parse(sql: String): Query = {
    try {
      val statement: Statement = CCJSqlParserUtil.parse(sql.replaceAll("\\|"," "), new Consumer[CCJSqlParser] {
        override def accept(t: CCJSqlParser): Unit = {
          t.withSquareBracketQuotation(true)
        }
      })
      val selectStatement: Select = statement.asInstanceOf[Select]
      //获取主表foreign_key
      val main_tab_foreign_key = getForeignKey(selectStatement)

      //获取主表从表表名
      val (main_table, from_table): (String, String) = getTableName(selectStatement)

      val (select_main_clause
      , from_table_fields
      , from_table_fields_alias) = getSelectClause(selectStatement, from_table, main_table, main_tab_foreign_key)

      //获取from_table where条件字段
      val filter_field = getFilterField(selectStatement, from_table_fields, from_table_fields_alias)

      Query(select_main_clause
        , from_table
        , main_tab_foreign_key
        , from_table_fields
        , from_table_fields_alias
        , filter_field)

    } catch {
      case e: Exception => return null
    }

  }

  def getForeignKey(selectStatement: Select) = {
    import scala.collection.JavaConversions._
    val joins: util.List[Join] = selectStatement.getSelectBody(classOf[PlainSelect]).getJoins
    joins(0)
      .getOnExpression()
      .toString
      .replaceAll(".*\\.((\\w+)?index\\w+).*", "$1")
  }

  def getFilterField(selectStatement: Select, from_table_fields: List[String], from_table_fields_alias: List[String]): String = {
    val where: Expression = selectStatement.getSelectBody(classOf[PlainSelect]).getWhere
    var field = if (where != null) where.toString.replaceAll("\\w+\\.(\\w+).*", "$1") else null

    var filter: String = null
    if (field != null) {
      (0 to from_table_fields.size - 1).foreach(field_index => {
        if (field.equals(from_table_fields(field_index)))
          filter = String.format("%s is not null", from_table_fields_alias(field_index))
      })
    }

    filter
  }

  def getTableName(selectStatement: Select): (String, String) = {
    import scala.collection.JavaConversions._
    val tablesNamesFinder: TablesNamesFinder = new TablesNamesFinder
    val tabList: util.List[String] = tablesNamesFinder.getTableList(selectStatement)

    val main_tab = tabList(0)
    //正则获取hbase表名
    //fbg_dwh_ods_ods_test.out_hbase_order_operation_time -> order_operation_time
    val from_tab = tabList(1).replaceAll(".*hbase_(\\w+)", "$1")

    (main_tab, from_tab)
  }


  def getSelectClause(selectStatement: Select
                      , from_table: String
                      , main_table: String
                      , main_tab_foreign_key: String): (String, List[String], List[String]) = {

    val builder: StringBuilder = new StringBuilder
    builder.append(Constants.PARSE_KEYWORD_SELECT)

    var from_table_fields: List[String] = List()
    var from_table_fields_alias: List[String] = List()

    import scala.collection.JavaConversions._
    for (selectItem <- selectStatement.getSelectBody.asInstanceOf[PlainSelect].getSelectItems) {
      selectItem.accept(new SelectItemVisitorAdapter() {
        override def visit(item: SelectExpressionItem): Unit = {

          if (!item.toString.contains(from_table)) {

            //正则去掉查询字段别名
            //orders.order_code as order_code -> order_code as order_code
            builder.append(String.format("\r\n %s ,", item.toString.replaceAll("(.*\\.)(.*)", "$2")))
          } else {
            //date_format(order_operation_time.submit_time, 'yyyy-MM-dd HH:mm:ss') as review_time_original -> submit_time AS review_time_original
            //order_operation_time.submit_time as review_time_original -> submit_time AS review_time_original
            val str = item.toString.replaceAll(".*\\w+\\.(\\w+).*( AS \\w+)", "$1$2")

            val field: Array[String] = str.split(Constants.PARSE_KEYWORD_AS)

            if (field.size > 1) {
              from_table_fields = from_table_fields.+:(field(0).trim)
              from_table_fields_alias = from_table_fields_alias.+:(field(1).trim)
            } else {
              from_table_fields = from_table_fields.+:(field(0).trim)
              from_table_fields_alias = from_table_fields_alias.+:(field(0).trim)
            }
          }
        }
      })
    }

    builder.append(String.format("\r\n %s ,", main_tab_foreign_key))
      .append(Constants.PARSE_KEYWORD_FROM)
      .append(main_table)

    //正则去掉from前的逗号
    //select
    //		order_code as order_code,
    //		channel_business_code as channel_business_code , from   ************** remove ',' ***********
    val select_main_clause = builder.toString().replaceAll("(.*)(, )(from.*)", "$1$3")


    (select_main_clause, from_table_fields.reverse, from_table_fields_alias.reverse)
  }

  case class Query(select_main_clause: String
                   , from_tab: String
                   , main_tab_foreign_key: String
                   , from_table_fields: List[String]
                   , from_table_fields_alias: List[String]
                   , filter: String) extends Serializable
}
