package org.apache.hadoop.yarn.server.timelineservice.storage.tictoapp;

import org.apache.hadoop.yarn.server.timelineservice.storage.common.BaseTable;

/**
 * The tic_to_app table has column family info. Info stores
 * tic to app mapping information
 *
 * Example domain table record:
 *
 * <pre>
 * |-------------------------------------------|
 * |  Row            | Column Family           |
 * |  key            | info                    |
 * |-------------------------------------------|
 * | tic             | id:appId                |
 * |-------------------------------------------|
 * </pre>
 */

public final class TicToAppTable extends BaseTable<TicToAppTable> {
}