package com.alibaba.datax.plugin.rdbms.reader.util;

import com.alibaba.datax.common.constant.CommonConstant;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.Constant;
import com.alibaba.datax.plugin.rdbms.reader.Key;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public final class ReaderSplitUtil {
    private static final Logger LOG = LoggerFactory.getLogger(ReaderSplitUtil.class);

    public static List<Configuration> doSplit(Configuration originalSliceConfig, int adviceNumber) {
        boolean isTableMode = originalSliceConfig.getBool(Constant.IS_TABLE_MODE);
        int eachTableShouldSplittedNumber = -1;
        if (isTableMode) {
            // adviceNumber这里是channel数量大小, 即datax并发task数量
            // eachTableShouldSplittedNumber是单表应该切分的份数, 向上取整可能和adviceNumber没有比例关系了已经
            eachTableShouldSplittedNumber = calculateEachTableShouldSplittedNumber(
                    adviceNumber, originalSliceConfig.getInt(Constant.TABLE_NUMBER_MARK));
        }

        String column = originalSliceConfig.getString(Key.COLUMN);
        String where = originalSliceConfig.getString(Key.WHERE, null);
        String splitPk = originalSliceConfig.getString(Key.SPLIT_PK, null);
        List<String> splitWhereConditions = originalSliceConfig.getList(Key.SPLIT_WHERE, String.class);

        List<Object> conns = originalSliceConfig.getList(Constant.CONN_MARK, Object.class);

        List<Configuration> splittedConfigs = new ArrayList<>();

        for (int i = 0, len = conns.size(); i < len; i++) {
            Configuration sliceConfig = originalSliceConfig.clone();

            Configuration connConf = Configuration.from(conns.get(i).toString());
            String jdbcUrl = connConf.getString(Key.JDBC_URL);
            sliceConfig.set(Key.JDBC_URL, jdbcUrl);

            // 抽取 jdbcUrl 中的 ip/port 进行资源使用的打标，以提供给 core 做有意义的 shuffle 操作
            sliceConfig.set(CommonConstant.LOAD_BALANCE_RESOURCE_MARK, DataBaseType.parseIpFromJdbcUrl(jdbcUrl));

            sliceConfig.remove(Constant.CONN_MARK);

            Configuration tempSlice;

            // 说明是配置的 table 方式
            if (isTableMode) {
                // 已在之前进行了扩展和`处理，可以直接使用
                List<String> tables = connConf.getList(Key.TABLE, String.class);

                Validate.isTrue(null != tables && !tables.isEmpty(), "您读取数据库表配置错误.");

                boolean useSplitPk = StringUtils.isNotBlank(splitPk);
                boolean useSplitWhere = splitWhereConditions != null && !splitWhereConditions.isEmpty() && StringUtils.isBlank(splitPk);

                if (useSplitPk) {
                    if (tables.size() == 1) {
                        //原来:如果是单表的，主键切分num=num*2+1
                        // splitPk is null这类的情况的数据量本身就比真实数据量少很多, 和channel大小比率关系时，不建议考虑
                        //eachTableShouldSplittedNumber = eachTableShouldSplittedNumber * 2 + 1;// 不应该加1导致长尾
                        
                        //考虑其他比率数字?(splitPk is null, 忽略此长尾)
                        //eachTableShouldSplittedNumber = eachTableShouldSplittedNumber * 5;

                        //为避免导入hive小文件 默认基数为5，可以通过 splitFactor 配置基数
                        // 最终task数为(channel/tableNum)向上取整*splitFactor
                        Integer splitFactor = originalSliceConfig.getInt(Key.SPLIT_FACTOR, Constant.SPLIT_FACTOR);
                        eachTableShouldSplittedNumber = eachTableShouldSplittedNumber * splitFactor;
                    }
                    // 尝试对每个表，切分为eachTableShouldSplittedNumber 份
                    for (String table : tables) {
                        tempSlice = sliceConfig.clone();
                        tempSlice.set(Key.TABLE, table);

                        List<Configuration> splittedSlices = SingleTableSplitUtil
                                .splitSingleTable(tempSlice, eachTableShouldSplittedNumber);
                        // 每个表的切分，都是一个独立的配置
                        splittedConfigs.addAll(splittedSlices);
                    }
                } else if (useSplitWhere) {
                    for (String table : tables) {
                        for (String splitWhere : splitWhereConditions) {
                            tempSlice = sliceConfig.clone();
                            tempSlice.set(Key.TABLE, table);
                            String queryColumn = HintUtil.buildQueryColumn(jdbcUrl, table, column);
                            String querySql = buildQuerySqlWithWhere(queryColumn, table, where, splitWhere);
                            tempSlice.set(Key.QUERY_SQL, querySql);
                            LOG.info("Generated query SQL: {}", querySql);
                            splittedConfigs.add(tempSlice);
                        }
                    }
                } else {
                    for (String table : tables) {
                        tempSlice = sliceConfig.clone();
                        tempSlice.set(Key.TABLE, table);
                        String queryColumn = HintUtil.buildQueryColumn(jdbcUrl, table, column);
                        String querySql = buildQuerySqlWithWhere(queryColumn, table, where, null);
                        tempSlice.set(Key.QUERY_SQL, querySql);
                        LOG.info("Generated query SQL: {}", querySql);
                        splittedConfigs.add(tempSlice);
                    }
                }
            } else {
                // 说明是配置的 querySql 方式
                List<String> sqls = connConf.getList(Key.QUERY_SQL, String.class);
                // TODO 是否check 配置为多条语句？？
                for (String querySql : sqls) {
                    tempSlice = sliceConfig.clone();
                    tempSlice.set(Key.QUERY_SQL, querySql);
                    LOG.info("Generated query SQL: {}", querySql);
                    splittedConfigs.add(tempSlice);
                }
            }
        }

        return splittedConfigs;
    }

    public static Configuration doPreCheckSplit(Configuration originalSliceConfig) {
        Configuration queryConfig = originalSliceConfig.clone();
        boolean isTableMode = originalSliceConfig.getBool(Constant.IS_TABLE_MODE);
        String splitPk = originalSliceConfig.getString(Key.SPLIT_PK, null);
        String column = originalSliceConfig.getString(Key.COLUMN);
        String where = originalSliceConfig.getString(Key.WHERE, null);

        List<String> splitWhereConditions = originalSliceConfig.getList(Key.SPLIT_WHERE, String.class);

        List<Object> conns = queryConfig.getList(Constant.CONN_MARK, Object.class);

        for (int i = 0, len = conns.size(); i < len; i++) {
            Configuration connConf = Configuration.from(conns.get(i).toString());
            List<String> querys = new ArrayList<>();
            List<String> splitPkQuerys = new ArrayList<>();

            if (isTableMode) {
                // 已在之前进行了扩展和`处理，可以直接使用
                List<String> tables = connConf.getList(Key.TABLE, String.class);
                Validate.isTrue(null != tables && !tables.isEmpty(), "您读取数据库表配置错误.");

                boolean useSplitPk = StringUtils.isNotBlank(splitPk);
                boolean useSplitWhere = splitWhereConditions != null && !splitWhereConditions.isEmpty() && StringUtils.isBlank(splitPk);

                for (String table : tables) {
                    if (useSplitPk) {
                        splitPkQuerys.add(SingleTableSplitUtil.genPKSql(splitPk.trim(), table, where));
                    } else if (useSplitWhere) {
                        for (String splitWhere : splitWhereConditions) {
                            String querySql = buildQuerySqlWithWhere(column, table, where, splitWhere);
                            querys.add(querySql);
                            LOG.info("Generated query SQL: {}", querySql);
                        }
                    } else {
                        String querySql = buildQuerySqlWithWhere(column, table, where, null);
                        querys.add(querySql);
                        LOG.info("Generated query SQL: {}", querySql);
                    }
                }
                if (!splitPkQuerys.isEmpty()) {
                    connConf.set(Key.SPLIT_PK_SQL, splitPkQuerys);
                }
                connConf.set(Key.QUERY_SQL, querys);
                queryConfig.set(String.format("connection[%d]", i), connConf);
            } else {
                List<String> sqls = connConf.getList(Key.QUERY_SQL, String.class);
                querys.addAll(sqls);
                connConf.set(Key.QUERY_SQL, querys);
                queryConfig.set(String.format("connection[%d]", i), connConf);
            }
        }
        return queryConfig;
    }

    private static int calculateEachTableShouldSplittedNumber(int adviceNumber, int tableNumber) {
        double tempNum = 1.0 * adviceNumber / tableNumber;

        return (int) Math.ceil(tempNum);
    }

    private static String buildQuerySqlWithWhere(String columns, String table, String where, String additionalWhere) {
        StringBuilder sb = new StringBuilder("SELECT ");
        sb.append(columns).append(" FROM ").append(table);
        if (StringUtils.isNotBlank(where)) {
            sb.append(" WHERE ").append(where);
        }
        if (StringUtils.isNotBlank(additionalWhere)) {
            if (sb.toString().contains("WHERE")) {
                sb.append(" AND ").append(additionalWhere);
            } else {
                sb.append(" WHERE ").append(additionalWhere);
            }
        }
        return sb.toString();
    }
}