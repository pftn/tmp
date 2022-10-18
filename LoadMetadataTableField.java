package com.ztn.metadata.collection.service;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.zt.dp.common.utils.encrypt.AESUtil;
import com.ztn.metadata.collection.util.DateUtil;
import com.ztn.metadata.collection.util.JdbcUtil;
import com.ztn.metadata.collection.util.PropUtil;
import com.ztn.metadata.collection.util.TxtUtil;
import org.apache.flink.table.types.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Locale;
import java.util.regex.Pattern;

/**
 * 加载元数据从metadata_aspect_v2表到metadata_table,metadata_field表
 * 记录表和字段变更到metadata_table_change,metadata_field_change表
 */
public class LoadMetadataTableField {
    private static final Logger LOG = LoggerFactory.getLogger(LoadMetadataTableField.class);

    public static void main(String[] args) {
        String platform = args[0].trim();
        String sourceCode = args[1].trim();
        String tablesSpecified = args[2].trim();
        String ingestFile = args[3].trim();
        String sourceDBUrl = args[4].trim();
        String sourceDBUsr = args[5].trim();
        String sourceDBPW = "";
        if (args.length > 6) {
            sourceDBPW = args[6].trim();
        }
        LoadMetadataTableField load = new LoadMetadataTableField();

        String ingestRes = TxtUtil.readLines(ingestFile);
        String ingestedDBs = ingestRes.split("\\n")[0];
        String ingestedTables = ingestRes.split("\\n")[1];
        load.exec(platform, sourceCode, tablesSpecified, ingestedDBs, ingestedTables, sourceDBUrl, sourceDBUsr, sourceDBPW);
    }

    public void exec(String platform, String sourceCode, String tablesSpecified, String ingestedDBs, String ingestedTables, String sourceDBUrl, String sourceDBUsr, String sourceDBPW) {
        String dbs = ingestedDBs.replace(",", "','");
        if ("[.*]".equals(tablesSpecified)) {
            if (bakMetadataTableFieldForChange(sourceCode, dbs) && replaceMetadataInfo(platform, sourceCode, tablesSpecified, ingestedDBs, ingestedTables, sourceDBUrl, sourceDBUsr, sourceDBPW)) {
                boolean status = addMetadataTableFieldChange(sourceCode, dbs);
                LOG.info("addMetadataTableFieldChange for platform: {} sourceCode: {} dbs: {} success: {}", platform, sourceCode, ingestedDBs, status);
            }
        } else {
            replaceMetadataInfo(platform, sourceCode, tablesSpecified, ingestedDBs, ingestedTables, sourceDBUrl, sourceDBUsr, sourceDBPW);
        }
    }

    public boolean bakMetadataTableFieldForChange(String sourceCode, String dbs) {
        //bak metadata table field for change
        if (JdbcUtil.execUpdateCollectDB(SqlStatement.deleteMetadataTableHistory(sourceCode, dbs)) && JdbcUtil.execUpdateCollectDB(SqlStatement.deleteMetadataFieldHistory(sourceCode, dbs))) {
            return JdbcUtil.execUpdateCollectDB(SqlStatement.bakMetadataTableForChange(sourceCode, dbs)) &&
                    JdbcUtil.execUpdateCollectDB(SqlStatement.bakMetadataFieldForChange(sourceCode, dbs));
        }
        return false;
    }

    public boolean addMetadataTableFieldChange(String sourceCode, String dbs) {
        return JdbcUtil.execUpdateCollectDB(SqlStatement.addMetadataTableChange(sourceCode, dbs)) &&
                JdbcUtil.execUpdateCollectDB(SqlStatement.addMetadataFieldChange(sourceCode, dbs));
    }

    public boolean replaceMetadataInfo(String platform, String sourceCode, String tablesSpecified, String dbs, String ingestedTables, String sourceDBUrl, String sourceDBUsr, String sourceDBPW) {
        JdbcUtil jdbcUtil = JdbcUtil.getInstance();
        Connection sourceConn = null;
        if ("mysql,oracle,doris,hive,saphana".contains(platform)) {
            try {
                sourceConn = jdbcUtil.getConnection(sourceDBUrl, sourceDBUsr, sourceDBPW);
            } catch (ClassNotFoundException | SQLException e) {
                LOG.error("error", e);
            }
        }
        if (JdbcUtil.execUpdateCollectDB(SqlStatement.updateSchemasDeleteFlag(sourceCode))) {
            //写表metadata_schema
            StringBuilder replaceSchemaSql = new StringBuilder();
            String[] dbArr = dbs.split(",");
            int schemaNum = dbArr.length;
            replaceSchemaSql.append("insert into metadata_schema (data_source_id, data_source_code, schema_info, is_deleted, create_date, last_update_date) values ");
            for (int i = 0; i < schemaNum; i++) {
                if (i != 0) {
                    replaceSchemaSql.append(",");
                }
                replaceSchemaSql.append("(0, '" + sourceCode + "', '" + dbArr[i] + "',0, current_timestamp(), current_timestamp())");
            }
            replaceSchemaSql.append("on duplicate key update data_source_id=values(data_source_id),data_source_code=values(data_source_code),schema_info=values(schema_info),is_deleted=values(is_deleted),create_date=values(create_date),last_update_date=values(last_update_date)");
            LOG.debug("replace metadata schema sql is: {}", replaceSchemaSql);
            Boolean updatedSchema = JdbcUtil.execUpdateCollectDB(replaceSchemaSql.toString());
            LOG.info("replace metadata schema for platform: {} sourceCode: {} dbs: {} success: {}", platform, sourceCode, dbs, updatedSchema);
        }

        int ingested = 0;
        int removed = 0;
        StringBuilder replaceTablesSql = new StringBuilder();
        StringBuilder replaceFieldsSql = new StringBuilder();
        StringBuilder updateTableDeleteFlagSql = new StringBuilder();
        StringBuilder replaceTablePropertySql = new StringBuilder();

        if ("saphana,doris".contains(platform)) {
            JSONArray tables = jdbcUtil.execQueryToJson(sourceConn, SqlStatement.getTables(platform));
            if (0 == tables.size()) {
                LOG.error("no table found for {}, please check data source", sourceCode);
                if (null != sourceConn) jdbcUtil.close(sourceConn);
                return false;
            }
            for (Object o : tables) {
                JSONObject table = JSONObject.parseObject(o.toString());
                if (ingested == 0) {
                    replaceTablesSql.append("replace into metadata_table (db_id, data_source_code,  col_schema, col_table, col_table_type, col_table_desc, col_table_create_date, col_table_last_update_date, col_table_num, is_deleted, col_create_date, col_last_update_date) values");
                } else {
                    replaceTablesSql.append(",");
                }
                String desc = table.getString("table_comment");
                desc = null == desc ? null : "'" + desc.replaceAll("[\t\n]", " ").replace("'", "") + "'";

                if ("saphana".equals(platform)) {
                    replaceTablesSql.append("('1','" + sourceCode + "','" + table.getString("schema_name") + "','" + table.getString("table_name") + "','" + table.getString("table_type") + "'," + desc + ",'" + table.getTimestamp("table_create_time") + "',NULL,NULL,0,now(),now())");
                } else if ("doris".equals(platform)) {
                    String updateTime = "'" + table.getTimestamp("table_update_time") + "'";
                    updateTime = updateTime.equals("'null'") ? "NULL" : updateTime;
                    BigInteger rowsNum = table.getBigInteger("table_rows");
                    replaceTablesSql.append("('1','" + sourceCode + "','" + table.getString("schema_name") + "','" + table.getString("table_name") + "','" + table.getString("table_type") + "'," + desc + ",'" + table.getTimestamp("table_create_time") + "'," + updateTime + ",'" + rowsNum + "',0,now(),now())");
                }

                ingested++;
            }
            JSONArray fields = jdbcUtil.execQueryToJson(sourceConn, SqlStatement.getFields(platform));

            int colCnt = fields.size();
            for (int i = 0; i < colCnt; i++) {
                JSONObject field = fields.getJSONObject(i);
                String desc = field.getString("field_comment");
                desc = null == desc ? null : "'" + desc.replaceAll("[\t\n]", " ").replace("'", "") + "'";
                int len = field.getInteger("length");
                int scale = field.getInteger("scale");
                int intLen = len - scale;
                String fieldType1 = field.getString("data_type_name");
                int nullable = field.getBoolean("is_nullable") ? 1 : 0;
                DataType type = FlinkDataTypeMapping.map(platform, fieldType1, len, scale);
                fieldType1 = null == fieldType1 ? null : "'" + fieldType1 + "'";
                String flinkFieldType = null == type ? fieldType1 : type.toString();
                flinkFieldType = nullable == 0 ? flinkFieldType + " NOT NULL" : flinkFieldType;
                if (i == 0) {
                    replaceFieldsSql.append("replace into metadata_field (table_id, data_source_code, col_schema, col_table, col_field_no, col_field_name, col_field_desc, col_field_default_val, col_field_type1, col_field_type2, col_field_type_flink, col_field_len, col_field_integer_len, col_field_decimal_len, col_field_partition_key, col_field_is_null, col_field_create_date, col_field_last_update_date, is_deleted, create_date, last_update_date) values");
                } else {
                    replaceFieldsSql.append(",");
                }
                replaceFieldsSql.append("(1,'" + sourceCode + "','" + field.getString("schema_name") + "','" + field.getString("table_name") + "'," + field.getInteger("position") + ",'" + field.getString("column_name") + "'," + desc + ",''," + fieldType1 + ",NULL,'" + flinkFieldType + "'," + len + "," + intLen + "," + scale + ",0," + nullable + ",NULL,NULL,0,now(),now())");
            }
        } else {
            String queryMetadataAspectSql = SqlStatement.queryMetadataAspectV2("doris".equals(platform) ? "mysql" : platform, "kafka".equals(platform) ? ingestedTables : dbs.replace(",", ".,") + ".", tablesSpecified);
            LOG.debug("queryMetadataAspectV2 sql is: {}", queryMetadataAspectSql);
            String metadataAspect = JdbcUtil.execQueryCollectionDB(queryMetadataAspectSql);
            LOG.info("queryMetadataAspectV2 res length is: {}", metadataAspect.length());
            if ("null".equals(metadataAspect)) {
                LOG.error("no table found in metadata_aspect_v2, please check data source and ingestion env");
                return false;
            }
            JSONArray metadataAspectArr = JSONArray.parseArray(metadataAspect);

            for (Object o : metadataAspectArr) {
                JSONObject table = JSONObject.parseObject(o.toString());

                //status.removed 表是否删除
                JSONObject status = table.getJSONObject("status");
                if (null != status && status.getBoolean("removed")) continue;

                //schemaMetadata
                JSONObject schemaMetadata = table.getJSONObject("schemaMetadata");
                if (null == schemaMetadata) continue; // case when kafka topic without schema

                //datasetKey.name schema名.表名
                String schemaDotTable = table.getJSONObject("datasetKey").getString("name");
                String schema = "kafka".equals(platform) ? platform : schemaDotTable.substring(0, schemaDotTable.indexOf("."));
                String tableName = "kafka".equals(platform) ? schemaDotTable : schemaDotTable.substring(schemaDotTable.indexOf(".") + 1);
                LOG.info("MetadataAspectV2's table: {}", schemaDotTable);

                //datasetProperties.customProperties.description 表描述
                JSONObject datasetProperties = table.getJSONObject("datasetProperties");
                JSONObject customProperties = null == datasetProperties ? null : datasetProperties.getJSONObject("customProperties");
                String tableDesc = null == datasetProperties ? null : datasetProperties.getString("description");

                //datasetProperties.customProperties.view_definition 视图描述
                Boolean isView = null == customProperties ? null : customProperties.getBoolean("is_view");
                String viewDefine = null == customProperties ? null : customProperties.getString("view_definition");
                tableDesc = null != isView && isView ? viewDefine : tableDesc;
                tableDesc = null == tableDesc ? null : "'" + tableDesc.replaceAll("[\t\n]", " ").replace("'", "") + "'";

                //表类型
                String tableType = "kafka".equals(platform) ? "MESSAGE" : (null != isView && isView ? "VIEW" : "TABLE");

                //datasetProperties.customProperties.CreateTime 表创建时间
                String tableCreateTime = DateUtil.formatDateTime(null == customProperties ? null : customProperties.getString("CreateTime:"));
                tableCreateTime = null == tableCreateTime ? null : "'" + tableCreateTime + "'";

                //datasetProperties.customProperties."Table Parameters: transient_lastDdlTime" 表最后时间
                String tableLastTime = DateUtil.getDateTime(null == customProperties ? null : customProperties.getLong("Table Parameters: transient_lastDdlTime"));
                tableLastTime = null == tableLastTime ? null : "'" + tableLastTime + "'";

                //datasetProperties.customProperties."Table Parameters: numRows" 表行数
                BigInteger numRows = null == customProperties ? null : customProperties.getBigInteger("Table Parameters: numRows");
                String ingestedStr = ingestedTables + ",";
                if (ingestedStr.contains(schemaDotTable + ",")) {
                    //schemaMetadata.fields 字段
                    JSONArray fieldsArr = schemaMetadata.getJSONArray("fields");

                    if (ingested == 0) {
                        replaceTablesSql.append("replace into metadata_table (db_id, data_source_code,  col_schema, col_table, col_table_type, col_table_desc, col_table_create_date, col_table_last_update_date, col_table_num, is_deleted, col_create_date, col_last_update_date) values");
                        replaceFieldsSql.append("replace into metadata_field (table_id, data_source_code, col_schema, col_table, col_field_no, col_field_name, col_field_desc, col_field_default_val, col_field_type1, col_field_type2, col_field_type_flink, col_field_len, col_field_integer_len, col_field_decimal_len, col_field_partition_key, col_field_is_null, col_field_create_date, col_field_last_update_date, is_deleted, create_date, last_update_date) values");
                        if ("doris,mysql,kafka".contains(platform)) {
                            replaceTablePropertySql.append("replace into metadata_table_properties (data_source_code, col_schema, col_table, prop_key, prop_value, create_time, last_update_time) values ");
                        }
                    } else {
                        replaceTablesSql.append(",");
                        if ("doris,mysql,kafka".contains(platform)) replaceTablePropertySql.append(",");
                    }
                    replaceTablesSql.append("('1','" + sourceCode + "','" + schema + "','" + tableName + "','" + tableType + "'," + tableDesc + "," + tableCreateTime + "," + tableLastTime + "," + numRows + ",0,now(),now())");

                    if ("doris".equals(platform)) {
                        String feNodes = sourceDBUrl.contains("jdbc:mysql://") ? sourceDBUrl.replace("jdbc:mysql://", "") : sourceDBUrl;
                        feNodes = feNodes.contains("/") ? feNodes.substring(0, feNodes.indexOf("/")) : feNodes;
                        feNodes = feNodes.substring(0, feNodes.indexOf(":") + 1) + PropUtil.getProp("DORIS_FE_HTTP_PORT");
                        replaceTablePropertySql.append("('" + sourceCode + "','" + schema + "','" + tableName + "','connector','" + platform + "',now(),now()),");
                        replaceTablePropertySql.append("('" + sourceCode + "','" + schema + "','" + tableName + "','fenodes','" + feNodes + "',now(),now()),");
                        replaceTablePropertySql.append("('" + sourceCode + "','" + schema + "','" + tableName + "','table.identifier','" + schemaDotTable + "',now(),now()),");
                        replaceTablePropertySql.append("('" + sourceCode + "','" + schema + "','" + tableName + "','username','" + sourceDBUsr + "',now(),now()),");
                        replaceTablePropertySql.append("('" + sourceCode + "','" + schema + "','" + tableName + "','password','" + AESUtil.encrypt(sourceDBPW, PropUtil.getProp("SECRET_KEY")) + "',now(),now()),");
                        replaceTablePropertySql.append("('" + sourceCode + "','" + schema + "','" + tableName + "','doris.request.retries','5',now(),now()),");
                        replaceTablePropertySql.append("('" + sourceCode + "','" + schema + "','" + tableName + "','doris.request.connect.timeout.ms','300000',now(),now()),");
                        replaceTablePropertySql.append("('" + sourceCode + "','" + schema + "','" + tableName + "','doris.request.read.timeout.ms','300000',now(),now()),");
                        replaceTablePropertySql.append("('" + sourceCode + "','" + schema + "','" + tableName + "','doris.request.tablet.size','1',now(),now()),");
                        replaceTablePropertySql.append("('" + sourceCode + "','" + schema + "','" + tableName + "','doris.batch.size','4096',now(),now()),");
                        replaceTablePropertySql.append("('" + sourceCode + "','" + schema + "','" + tableName + "','doris.exec.mem.limit','21474836480',now(),now())");
                    } else if ("mysql".equals(platform)) {
                        replaceTablePropertySql.append("('" + sourceCode + "','" + schema + "','" + tableName + "','connector','jdbc',now(),now()),");
                        replaceTablePropertySql.append("('" + sourceCode + "','" + schema + "','" + tableName + "','url','" + sourceDBUrl + "/" + schema + "',now(),now()),");
                        replaceTablePropertySql.append("('" + sourceCode + "','" + schema + "','" + tableName + "','table-name','" + tableName + "',now(),now()),");
                        replaceTablePropertySql.append("('" + sourceCode + "','" + schema + "','" + tableName + "','username','" + sourceDBUsr + "',now(),now()),");
                        replaceTablePropertySql.append("('" + sourceCode + "','" + schema + "','" + tableName + "','password','" + AESUtil.encrypt(sourceDBPW, PropUtil.getProp("SECRET_KEY")) + "',now(),now())");
                    } else if ("kafka".equals(platform)) {
                        replaceTablePropertySql.append("('" + sourceCode + "','" + schema + "','" + tableName + "','connector','kafka',now(),now()),");
                        replaceTablePropertySql.append("('" + sourceCode + "','" + schema + "','" + tableName + "','topic','" + tableName + "',now(),now()),");
                        replaceTablePropertySql.append("('" + sourceCode + "','" + schema + "','" + tableName + "','properties.bootstrap.servers','" + sourceDBUrl + "',now(),now()),");
                        replaceTablePropertySql.append("('" + sourceCode + "','" + schema + "','" + tableName + "','properties.group.id','group-" + sourceDBUsr + "',now(),now()),");
                        replaceTablePropertySql.append("('" + sourceCode + "','" + schema + "','" + tableName + "','scan.startup.mode','earliest-offset',now(),now()),");
                        replaceTablePropertySql.append("('" + sourceCode + "','" + schema + "','" + tableName + "','format','json',now(),now())");
                    }

                    //hive表分区
                    String partitions = "";
                    if ("hive".equals(platform)) {
                        partitions = jdbcUtil.getPartition(sourceConn, "desc " + schemaDotTable);
                        LOG.info("partitions for table: {} is: {}", schemaDotTable, partitions);
                    }

                    int colCnt = fieldsArr.size();
                    for (int j = 0; j < colCnt; j++) {
                        JSONObject fieldJson = fieldsArr.getJSONObject(j);

                        //字段名
                        String fieldName = fieldJson.getString("fieldPath");

                        //字段描述
                        String desc = fieldJson.getString("description");
                        desc = null == desc ? null : "'" + desc.replaceAll("[\t\n]", " ").replace("'", "") + "'";

                        //字段类型1 字段类型2
                        String fieldType1;
                        String fieldType2 = fieldJson.getString("nativeDataType").replaceAll("'", "");
                        if ("kafka".equals(platform)) {
                            fieldType1 = fieldName.substring(fieldName.lastIndexOf("[") + 1, fieldName.lastIndexOf("]")).replace("type=", "").trim();
                            fieldName = fieldType2;
                            fieldType2 = null;
                        } else {
                            fieldType1 = fieldType2.contains("(") ? fieldType2.substring(0, fieldType2.indexOf("(")) : fieldType2;
                        }
                        fieldType2 = null == fieldType2 ? null : "'" + fieldType2 + "'";

                        //是否分区字段
                        int isPartition = "hive".equals(platform) ? partitions.contains(fieldName + ",") ? 1 : 0 : 0;

                        //字段是否可空
                        int nullable = isPartition == 1 ? 0 : (fieldJson.getBoolean("nullable") ? 1 : 0);

                        //decimal field长度 field整型部分长度 field小数部分长度
                        int precision = 0;
                        int intLen = 0;
                        int scale = 0;
                        if ("mysql,oracle".contains(platform) && Pattern.matches(".*precision=\\d+, scale=\\d+.*", fieldType2)) {
                            precision = Integer.parseInt(fieldType2.substring(fieldType2.indexOf("precision=") + 10, fieldType2.indexOf(", scale=")));
                            String sub = fieldType2.substring(fieldType2.indexOf("scale=") + 6);
                            scale = Integer.parseInt(sub.substring(0, sub.indexOf(",") > 0 ? sub.indexOf(",") : sub.indexOf(")")));
                            intLen = precision - scale;
                        }
                        if ("hive".equals(platform) && fieldType2.contains("decimal")) {
                            precision = Integer.parseInt(fieldType2.substring(fieldType2.indexOf("(") + 1, fieldType2.indexOf(",")));
                            scale = Integer.parseInt(fieldType2.substring(fieldType2.indexOf(",") + 1, fieldType2.indexOf(")")));
                            intLen = precision - scale;
                        }

                        //转换成flink字段类型
                        int len = "oracle".equals(platform) && fieldType1.contains("CHAR") ? Integer.parseInt(fieldType2.substring(fieldType2.indexOf("length=") + 7, fieldType2.indexOf(")"))) : precision;
                        DataType type = FlinkDataTypeMapping.map(platform, fieldType1, len, scale);
                        String flinkFieldType = null == type ? fieldType1.toUpperCase(Locale.ROOT) : type.toString();
                        flinkFieldType = "doris".equals(platform) && "DECIMAL(1, 0)".equals(flinkFieldType) ? "DECIMAL(27, 9)" : flinkFieldType;
                        flinkFieldType = nullable == 0 ? flinkFieldType + " NOT NULL" : flinkFieldType;

                        if (ingested != 0 || j != 0) replaceFieldsSql.append(",");
                        replaceFieldsSql.append("(1,'" + sourceCode + "','" + schema + "','" + tableName + "'," + (j + 1) + ",'" + fieldName + "'," + desc + ",'','" + fieldType1 + "'," + fieldType2 + ",'" + flinkFieldType + "'," + precision + "," + intLen + "," + scale + "," + isPartition + ",'" + nullable + "',NULL,NULL,0,now(),now())");
                    }
                    ingested++;
                } else {
                    if (removed == 0) {
                        updateTableDeleteFlagSql.append("update metadata_table set is_deleted = 1 where data_source_code = '" + sourceCode + "' and (");
                    } else {
                        updateTableDeleteFlagSql.append(" or ");
                    }
                    updateTableDeleteFlagSql.append("(col_schema = '" + schema + "' and col_table = '" + tableName + "')");
                    removed++;
                }
            }
        }

        LOG.info("ingested tables: {} removed tables: {}", ingested, removed);
        boolean updatedField = false;
        boolean updatedTableProperties;
        boolean updateTableDeleteFlag;
        if (removed > 0 || "saphana,doris".contains(platform)) {
            if ("saphana,doris".contains(platform)) {
                String updateTableDeleteFlagSql1 = SqlStatement.updateTablesDeleteFlag(sourceCode, dbs.replace(",", "','"));
                LOG.debug("update table delete flag sql is: {}", updateTableDeleteFlagSql1);
                updateTableDeleteFlag = JdbcUtil.execUpdateCollectDB(updateTableDeleteFlagSql1);
            } else {
                updateTableDeleteFlagSql.append(")");
                LOG.debug("update table delete flag sql is: {}", updateTableDeleteFlagSql);
                updateTableDeleteFlag = JdbcUtil.execUpdateCollectDB(updateTableDeleteFlagSql.toString());
            }
            LOG.info("update table delete flag for platform: {} sourceCode: {} dbs: {} success: {}", platform, sourceCode, dbs, updateTableDeleteFlag);
        } else {
            updateTableDeleteFlag = true;
        }
        if (ingested > 0 && updateTableDeleteFlag) {
            boolean updateFieldsDeleteFlag;
            if ("[.*]".equals(tablesSpecified)) {
                String updateFieldsDeleteFlagSql = SqlStatement.updateFieldsDeleteFlag(sourceCode, dbs.replace(",", "','"));
                LOG.debug("update fields delete flag sql is: {}", updateFieldsDeleteFlagSql);
                updateFieldsDeleteFlag = JdbcUtil.execUpdateCollectDB(updateFieldsDeleteFlagSql);
            } else { //if tables specified, no change process
                updateFieldsDeleteFlag = true;
            }
            LOG.debug("load metadata table sql is: {}", replaceTablesSql);
            LOG.debug("load metadata field sql is: {}", replaceFieldsSql);
            LOG.debug("load metadata table properties sql is: {}", replaceTablePropertySql);
            if (JdbcUtil.execUpdateCollectDB(replaceTablesSql.toString()) && updateFieldsDeleteFlag) {
                updatedField = JdbcUtil.execUpdateCollectDB(replaceFieldsSql.toString());
                LOG.info("load metadata table and field for platform: {} sourceCode:{} dbs: {} success: {}", platform, sourceCode, dbs, updatedField);
                if (updatedField && !replaceTablePropertySql.toString().isEmpty()) {
                    updatedTableProperties = JdbcUtil.execUpdateCollectDB(replaceTablePropertySql.toString());
                    LOG.info("load metadata table properties for platform: {} sourceCode:{} dbs: {} success: {}", platform, sourceCode, dbs, updatedTableProperties);
                }
            }
        }

        if (!"mysql,oracle,saphana".contains(platform)) {
            if (null != sourceConn) jdbcUtil.close(sourceConn);
            return updatedField;
        }

        //写表 metadata_pk_index
        StringBuilder replacePkIndexSql = new StringBuilder();
        String pkIndex;
        JSONArray pkIndexArr = new JSONArray();
        String getPkIndexSql = SqlStatement.getPkIndex(platform, dbs.replace(",", "','"));
        LOG.debug("getPkIndexSql sql is: {}", getPkIndexSql);
        if ("mysql".equals(platform)) {
            pkIndex = jdbcUtil.execQuery(sourceConn, getPkIndexSql);
            if (pkIndex.isEmpty()) {
                LOG.error("no pk and index found for sourceCode " + sourceCode + ", please check data source and ingestion env");
                if (null != sourceConn) jdbcUtil.close(sourceConn);
                return false;
            }
            pkIndexArr = JSONArray.parseArray(pkIndex);
        } else if ("oracle".equals(platform) || "saphana".equals(platform)) {
            pkIndexArr = jdbcUtil.execQueryToJson(sourceConn, getPkIndexSql);
            if (0 == pkIndexArr.size()) {
                LOG.error("no pk and index found for sourceCode " + sourceCode + ", please check data source and ingestion env");
                if (null != sourceConn) jdbcUtil.close(sourceConn);
                return false;
            }
        }
        if (null != sourceConn) jdbcUtil.close(sourceConn);
        int pkIndexCnt = pkIndexArr.size();
        replacePkIndexSql.append("replace into metadata_pk_index (table_id, data_source_code, col_schema, col_table, col_index, col_index_type, col_is_pk, col_is_uk, col_field_name, col_idx_create_date, col_idx_last_update_date, is_deleted, col_create_date, col_last_update_date) values ");

        for (int i = 0; i < pkIndexCnt; i++) {
            JSONObject table = JSONObject.parseObject(pkIndexArr.get(i).toString());
            String schema = table.getString("table_schema");
            String tableName = table.getString("table_name");
            String indexName = table.getString("index_name");
            String indexType1 = table.getString("index_type");
            int indexType = "FK".equals(indexType1) ? 4 : ("PK".equals(indexType1) ? 1 : ("UK".equals(indexType1) ? 2 : 3));
            int isUk = 2 == indexType ? 1 : 0;
            int isPk = 1 == indexType ? 1 : 0;
            String fieldName = table.getString("column_name");
            if (i != 0) {
                replacePkIndexSql.append(",");
            }
            replacePkIndexSql.append("(0,'" + sourceCode + "','" + schema + "','" + tableName + "','" + indexName + "'," + indexType + "," + isPk + "," + isUk + ",'" + fieldName + "',NULL,NULL,0,now(),now())");
        }

        LOG.debug("load metadata pk index sql is: {}", replacePkIndexSql);
        boolean updatedPkIndex = false;
        if (JdbcUtil.execUpdateCollectDB(SqlStatement.updatePkIndexDeleteFlag(sourceCode))) {
            updatedPkIndex = JdbcUtil.execUpdateCollectDB(replacePkIndexSql.toString());
            LOG.info("load metadata pk index for platform: {} sourceCode:{} dbs: {} success: {}", platform, sourceCode, dbs, updatedPkIndex);
        }

        return updatedField && updatedPkIndex;
    }
}
