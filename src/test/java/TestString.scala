


object TestString {

  def main(args: Array[String]): Unit = {
    val str =
      """
        |create table fbg_dwh_dwd_das.mid_hive_ascan_order_big_table_02 as
        |select
        |		orders.order_code as order_code
        |		,orders.channel_business_code as channel_business_code
        |		,orders.platform as platform
        |		,orders.update_time as update_time
        |		,orders.is_more_box
        |		,orders.warehouse_code as warehouse_code
        |		,orders.sm_code as sm_code
        |        ,orders.sc_code as sc_code
        |		,orders.customer_code as customer_code
        |        ,orders.tracking_number as tracking_number
        |		,date_format(order_operation_time.submit_time, 'yyyy-MM-dd HH:mm:ss') as review_time_original
        |		,date_format(order_operation_time.ship_time, 'yyyy-MM-dd HH:mm:ss') as check_out_time_original
        |    FROM fbg_dwh_dwd_das.mid_hive_ascan_order_big_table_01 orders
        |	left join fbg_dwh_ods_ods.out_hbase_order_operation_time order_operation_time
        |		   on orders.order_operation_time_index_rowkey_id = order_operation_time.rowkey_id
        |	where order_operation_time.submit_time is not null
        |""".stripMargin
        println(str)
//    println(String.format("after : %s",str.replaceAll("\\|"," ")))
  }

}
