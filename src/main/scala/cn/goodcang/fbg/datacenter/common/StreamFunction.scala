package cn.goodcang.fbg.datacenter.common

import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row

object StreamFunction {


  def dataRows2RowKeys(row: Row, query: SqlParser.Query): String = {
    if (StringUtils.isEmpty(row.getAs(query.main_tab_foreign_key))) {
      null
    } else {
      row.getAs[String](query.main_tab_foreign_key)
    }
  }
}
