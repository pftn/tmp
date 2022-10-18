package com.ztn.metadata.collection.service;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ztn.metadata.collection.model.MetadataCollectLog;
import com.ztn.metadata.collection.model.MetadataTable;
import com.ztn.metadata.collection.util.JdbcUtil;
import com.ztn.metadata.collection.util.PropUtil;
import com.ztn.metadata.collection.util.Sh;
import com.ztn.metadata.collection.util.TxtUtil;
import org.apache.commons.net.util.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

@Service(value = "metadataCollectionService")
public class MetadataCollectionService {
    private static final Logger LOG = LoggerFactory.getLogger(MetadataCollectionService.class);
    @Autowired
    MetadataCollectLogService metadataCollectLogService;

    @Autowired
    MetadataTableService metadataTableService;

    @Async("asyncTaskExecutor")
    public void execCollect(String platform, String sourceCode, String sourceDBUrl, String sourceDBUsr, String sourceDBPW, String requestId, Integer taskId, Integer executeType, String dbsSpecified, String tablesSpecified) {
        LOG.info("execCollect begin");
        MetadataCollectLog metadataCollectLog = new MetadataCollectLog();
        metadataCollectLog.setRequestId(requestId);
        metadataCollectLog.setCollectTaskId(taskId);
        metadataCollectLog.setExecuteType(Byte.valueOf(String.valueOf(executeType)));
        if ("saphana,doris".contains(platform)) {
            LoadMetadataTableField load = new LoadMetadataTableField();
            JdbcUtil jdbcUtil = JdbcUtil.getInstance();
            Connection sourceConn = null;
            StringBuilder dbs = new StringBuilder();
            try {
                sourceConn = jdbcUtil.getConnection(sourceDBUrl, sourceDBUsr, sourceDBPW);
                JSONArray dbsArr = jdbcUtil.execQueryToJson(sourceConn, SqlStatement.getSchemas(platform));
                if (0 == dbsArr.size()) {
                    metadataCollectLog.setLogContent("no db found");
                } else {
                    if ("[.*]".equals(dbsSpecified)) {
                        for (Object o : dbsArr) {
                            dbs.append(",").append(JSONObject.parseObject(o.toString()).getString("schema_name"));
                        }
                    } else {
                        dbsSpecified = dbsSpecified.substring(1, dbsSpecified.length() - 1);
                        String[] specified = dbsSpecified.split(",");
                        for (String specify : specified) {
                            for (Object o : dbsArr) {
                                String found = JSONObject.parseObject(o.toString()).getString("schema_name");
                                if (specify.equals(found + "$")) {
                                    dbs.append(",").append(found);
                                    break;
                                }
                            }
                        }
                    }

                    LOG.info("dbs for {} to ingest is: {}", sourceCode, dbs);
                    if (dbs.length() > 0) {
                        load.exec(platform, sourceCode, tablesSpecified, dbs.substring(1), "", sourceDBUrl, sourceDBUsr, sourceDBPW);
                        List<MetadataTable> metadataTables = metadataTableService.listMetadataTable(sourceCode, dbs.substring(1));
                        StringBuilder logContent = new StringBuilder();
                        StringBuilder ingestedTables = new StringBuilder();
                        StringBuilder removedTables = new StringBuilder();
                        logContent.append("ingested dbs: " + dbs.substring(1) + ";ingested tables: ");
                        int ingested = 0;
                        for (MetadataTable table : metadataTables) {
                            if (0 == table.getIsDeleted().intValue()) {
                                ingestedTables.append("," + table.getColSchema().trim() + "." + table.getColTable().trim());
                                ingested++;
                            } else {
                                removedTables.append("," + table.getColSchema().trim() + "." + table.getColTable().trim());
                            }
                        }
                        int removed = metadataTables.size() - ingested;
                        logContent.append(ingestedTables.substring(1));
                        logContent.append(";ingested tables: " + ingested);
                        logContent.append(" removed tables: " + removed);
                        metadataCollectLog.setLogContent(logContent.toString());

                        if ("doris".equals(platform) && (ingested > 0 || removed > 0)) {
                            if (ingested > 0) {
                                updateIsPartitionKey(platform, sourceCode, metadataTables, jdbcUtil);
                            }
                            StringBuilder ddlTxt = new StringBuilder();
                            ddlTxt.append(dbs.substring(1)).append("\n");
                            if (ingested > 0) {
                                ddlTxt.append(ingestedTables.substring(1));
                            }
                            ddlTxt.append("\n");
                            if (removed > 0) {
                                ddlTxt.append(removedTables.substring(1));
                            }
                            String uuid = UUID.randomUUID().toString().replace("-", "");
                            String resFileName = sourceCode + "_" + uuid + ".res";
                            TxtUtil.writeFile(ddlTxt.toString(), PropUtil.getProp("LOG_PATH") + "/ingest-res", resFileName);
                            String cmd = System.getProperty("user.dir") + "/bin/exec_spark_sql_ddl.sh " + resFileName;
                            LOG.info("sourceCode: {} exec_spark_sql_ddl.sh {} begin", sourceCode, resFileName);
                            HashMap<String, String> res = Sh.exec(cmd);
                            LOG.info("sourceCode: {} exec_spark_sql_ddl.sh log is {}", sourceCode, res);
                        }
                    } else {
                        LOG.info("dbsSpecified: {} not exist", dbsSpecified);
                        metadataCollectLog.setLogContent("dbsSpecified: " + dbsSpecified + " not exist");
                    }
                }
                metadataCollectLog.setCollectStatus(Byte.valueOf("1"));
            } catch (ClassNotFoundException | SQLException e) {
                metadataCollectLog.setCollectStatus(Byte.valueOf("0"));
                metadataCollectLog.setLogContent(e.getMessage());
            } finally {
                if (null != sourceConn) jdbcUtil.close(sourceConn);
            }
        } else {
            if ("kafka".equals(platform)) {
                if ("[.*]".equals(tablesSpecified) && !"[.*]".equals(dbsSpecified)) {
                    tablesSpecified = dbsSpecified; //kafka: specify db means specify topic(table)
                }
            }
            String cmd = System.getProperty("user.dir") + "/bin/load_metadata_aspect_v2.sh " + platform + " " + sourceCode + " " + dbsSpecified + " " + tablesSpecified + " " + sourceDBUrl + " " + sourceDBUsr + " " + sourceDBPW;
            LOG.info("begin exec load_metadata_aspect_v2.sh {}", sourceDBUrl);
            HashMap<String, String> res = Sh.exec(cmd);
            LOG.info("exec load_metadata_aspect_v2.sh log is {}", res);
            String exitValue = res.get("exitValue");
            StringBuilder content = new StringBuilder();
            String log = res.get("log");
            String[] logs = log.split("\n");
            if ("0".equals(exitValue)) {
                for (String line : logs) {
                    if (line.contains("ingested"))
                        content.append(line.substring(line.indexOf("ingested")).trim()).append(";");
                }
            } else {
                for (String line : logs) {
                    if (line.startsWith("ingesting... for") || line.startsWith("ingest status check") || line.contains("DEBUG"))
                        continue;
                    content.append(line);
                }
            }
            metadataCollectLog.setLogContent(content.toString());
            metadataCollectLog.setCollectStatus(Byte.valueOf(res.get("exitValue").equals("0") ? "1" : "0"));
        }
        metadataCollectLogService.addMetadataCollectLog(metadataCollectLog);
        LOG.info("execCollect end");
    }

    public void updateIsPartitionKey(String platform, String sourceCode, List<MetadataTable> metadataTables, JdbcUtil jdbcUtil) {
        StringBuilder updateIsPartitionKey = new StringBuilder();
        updateIsPartitionKey.append("update metadata_field set col_field_partition_key = 1 where ");
        int rowSumToUpdate = 0;
        for (MetadataTable table : metadataTables) {
            if (0 == table.getIsDeleted().intValue()) {
                String schema = table.getColSchema().trim();
                String tableName = table.getColTable().trim();
                String keysStr = getPartitionKey(schema, tableName);
                if (!keysStr.equals("")) {
                    String[] keys = keysStr.split(",");
                    for (String key : keys) {
                        updateIsPartitionKey.append("or (data_source_code = '").append(sourceCode).append("' and col_schema = '")
                                .append(schema).append("' and col_table = '").append(tableName).append("' and col_field_name='").append(key)
                                .append("')");
                    }
                    rowSumToUpdate += keys.length;
                }
            }
        }
        String updateSql = updateIsPartitionKey.toString().replace("where or (", "where (");
        LOG.info("updateIsPartitionKey sql is: {}", updateSql);
        if (rowSumToUpdate > 0) {
            int updateRows = jdbcUtil.execUpdate(updateSql);
            LOG.info("updateIsPartitionKey for platform {} sourceCode {} success: {}. Updated rows expect: {} actual: {}", platform, sourceCode, (updateRows == rowSumToUpdate), rowSumToUpdate, updateRows);
        }
    }

    public HashMap<String, String> getPartitionKeyAndAttribute(String schema, String table) {
        String getDdlUrl = "http://" + PropUtil.getProp("DORIS_FE_NODES") + ":" + PropUtil.getProp("DORIS_FE_HTTP_PORT") +
                "api/_get_ddl?db=" + schema + "&table=" + table;
        HttpHeaders headers = new HttpHeaders();
        String auth = PropUtil.getProp("DORIS_USER") + ":" + PropUtil.getProp("DORIS_PW");
        byte[] encodeAuth = Base64.encodeBase64(auth.getBytes(StandardCharsets.US_ASCII));
        headers.set("Authorization", "Basic " + new String(encodeAuth));
        headers.set("Accept", "*/*");
        HttpEntity requestEntity = new HttpEntity(headers);
        RestTemplate restTemplate = new RestTemplate();
        ResponseEntity<JSONObject> response = restTemplate.exchange(getDdlUrl, HttpMethod.GET, requestEntity, JSONObject.class, new HashMap<>());
        JSONObject responseBody = response.getBody();
        LOG.debug("getPartitionKey http response is: {}", responseBody.toJSONString());
        HashMap<String, String> res = new HashMap<>();
        String keys = "";
        HashMap<String, String> attributes = new HashMap<>();
        if (JSONObject.parseObject(responseBody.toString()).getString("msg").equals("success")) {
            String ddl = JSONObject.parseObject(responseBody.toString()).getJSONObject("data").getString("create_table");
            LOG.debug("ddl for {}.{} is: {}", schema, table, ddl);
            if (null != ddl && ddl.contains("PARTITION BY")) {
                keys = ddl.substring(ddl.indexOf("PARTITION BY"));
                keys = keys.substring(keys.indexOf("(") + 1, keys.indexOf(")")).replace("`", "").replace(" ", "");
            }
            getAttributesFromDdl(ddl.substring(ddl.indexOf(") ENGINE=")));
        }
        LOG.info("getPartitionKey for {}.{} is: {}", schema, table, keys);
        res.put("key", keys);

        return res;
    }

    public HashMap<String, String> getAttributesFromDdl(String ddl) {
        HashMap<String, String> attributes = new HashMap<>();
        String[] keys = new String[]{") ENGINE=", "AGGREGATE KEY", "UNIQUE KEY", "DUPLICATE KEY", "COMMENT", "PARTITION BY", "DISTRIBUTED BY", "ROLLUP", "PROPERTIES", "BROKER PROPERTIES"};
        String sub;
        int begin;
        for (String key : keys) {
            begin = ddl.indexOf(key) + key.length();
            sub = ddl.substring(begin);
            int end = sub.indexOf("\n");
            String value = sub.substring(0, end);
            if (!key.equals("PARTITION BY") && !key.equals("ROLLUP") && !key.equals("PROPERTIES") && !key.equals("BROKER PROPERTIES")) {
                attributes.put(key.equals(") ENGINE=") ? "ENGINE" : key, value.trim().replace("\\", ""));
            } else if (key.equals("PARTITION BY")) {
                String tmp;
                do {
                    sub = sub.substring(end + "\n".length());
                    end = sub.indexOf("\n");
                    tmp = sub.substring(end);
                } while (tmp.contains("PARTITION"));

            }

        }
        return attributes;
    }
}
