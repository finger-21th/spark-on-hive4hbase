package cn.goodcang.fbg.datacenter.common

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.conf.Configuration

/**
 * Wrapper for HBaseConfiguration
 */
class HBaseConfig(defaults: Configuration) extends Serializable {
  def get: Configuration = HBaseConfiguration.create(defaults)
}

/**
 * Factories to generate HBaseConfig instances.
 *
 * We can generate an HBaseConfig
 * - from an existing HBaseConfiguration
 * - from a sequence of String key/value pairs
 * - from an existing object with a `rootdir` and `quorum` members
 *
 * The two latter cases are provided for simplicity
 * (ideally a client should not have to deal with the native
 * HBase API).
 *
 * The last constructor contains the minimum information to
 * be able to read and write to the HBase cluster. It can be used
 * in tandem with a case class containing job configuration.
 */
object HBaseConfig {
  def apply(conf: Configuration): HBaseConfig = new HBaseConfig(conf)

  def apply(options: (String, String)*): HBaseConfig = {
    val conf = HBaseConfiguration.create

    for ((key, value) <- options) { conf.set(key, value) }

    apply(conf)
  }

  def apply(zk:String): HBaseConfig = {
    val conf = HBaseConfiguration.create

    conf.set(Constants.ZOOKEEPER_QUORUM, zk)

    apply(conf)
  }

  def apply(conf: {  def quorum: String }): HBaseConfig = apply(
    "hbase.zookeeper.quorum" -> conf.quorum)
}