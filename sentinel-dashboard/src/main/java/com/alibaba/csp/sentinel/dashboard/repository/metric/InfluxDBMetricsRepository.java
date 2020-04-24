package com.alibaba.csp.sentinel.dashboard.repository.metric;


import com.alibaba.csp.sentinel.dashboard.datasource.entity.MetricEntity;
import com.alibaba.csp.sentinel.dashboard.datasource.entity.influxdb.SentinelMetric;
import com.alibaba.csp.sentinel.dashboard.util.InfluxDBUtils;
import com.alibaba.csp.sentinel.util.StringUtil;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.commons.lang.time.DateUtils;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Point;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author lixiangqian
 * @since 2020/4/18 11:08 下午
 */
@Repository("influxDBMetricsRepository")
public class InfluxDBMetricsRepository implements MetricsRepository<MetricEntity> {

    private static final String DATE_FORMAT_PATTERN = "yyyy-MM-dd HH:mm:ss.SSS";

    private static final String SENTINEL_DATABASE = "sentinel_db";

    private static final String METRIC_MEASUREMENT = "sentinel_metric";

    @Override
    public void save(MetricEntity metric) {
        if (metric == null || StringUtil.isBlank(metric.getApp())) {
            return;
        }

        InfluxDBUtils.insert(SENTINEL_DATABASE, (database, influxDB) -> {
            if (metric.getId() == null) {
                metric.setId(System.currentTimeMillis());
            }
            doSave(influxDB, metric);
        });
    }

    @Override
    public void saveAll(Iterable<MetricEntity> metrics) {
        if (metrics == null) {
            return;
        }

        Iterator<MetricEntity> iterator = metrics.iterator();
        boolean next = iterator.hasNext();
        if (!next) {
            return;
        }

        InfluxDBUtils.insert(SENTINEL_DATABASE, (database, influxDB) -> {
            while (iterator.hasNext()) {
                MetricEntity metric = iterator.next();
                if (metric.getId() == null) {
                    metric.setId(System.currentTimeMillis());
                }
                doSave(influxDB, metric);
            }
        });
    }

    @Override
    public List<MetricEntity> queryByAppAndResourceBetween(String app, String resource, long startTime, long endTime) {
        List<MetricEntity> results = new ArrayList<MetricEntity>();
        if (StringUtil.isBlank(app)) {
            return results;
        }

        if (StringUtil.isBlank(resource)) {
            return results;
        }

        StringBuilder sql = new StringBuilder();
        sql.append("SELECT * FROM " + METRIC_MEASUREMENT);
        sql.append(" WHERE app=$app");
        sql.append(" AND resource=$resource");
        sql.append(" AND time>=$startTime");
        sql.append(" AND time<=$endTime");

        Map<String, Object> paramMap = new HashMap<>();
        paramMap.put("app", app);
        paramMap.put("resource", resource);
        paramMap.put("startTime", DateFormatUtils.format(DateUtils.addHours(new Date(startTime), -8).getTime(), DATE_FORMAT_PATTERN));
        paramMap.put("endTime", DateFormatUtils.format(DateUtils.addHours(new Date(endTime), -8).getTime(), DATE_FORMAT_PATTERN));

        List<SentinelMetric> sentinelMetrics = InfluxDBUtils.queryList(SENTINEL_DATABASE, sql.toString(), paramMap, SentinelMetric.class);

        if (CollectionUtils.isEmpty(sentinelMetrics)) {
            return results;
        }

        for (SentinelMetric sentinelMetric : sentinelMetrics) {
            results.add(convertToMetricEntity(sentinelMetric));
        }

        return results;
    }

    @Override
    public List<String> listResourcesOfApp(String app) {
        List<String> results = new ArrayList<>();
        if (StringUtil.isBlank(app)) {
            return results;
        }

        StringBuilder sql = new StringBuilder();
        sql.append("SELECT * FROM " + METRIC_MEASUREMENT);
        sql.append(" WHERE app=$app");
        sql.append(" AND time>=$startTime");


        Map<String, Object> paramMap = new HashMap<>();
        long startTime = System.currentTimeMillis() - 1000 * 60;
        paramMap.put("app", app);
        paramMap.put("startTime", DateFormatUtils.format(DateUtils.addHours(new Date(startTime), -8).getTime(), DATE_FORMAT_PATTERN));

        List<SentinelMetric> sentinelMetrics = InfluxDBUtils.queryList(SENTINEL_DATABASE, sql.toString(), paramMap, SentinelMetric.class);

        if (CollectionUtils.isEmpty(sentinelMetrics)) {
            return results;
        }

        List<MetricEntity> metricEntities = new ArrayList<MetricEntity>();
        for (SentinelMetric sentinelMetric : sentinelMetrics) {
            metricEntities.add(convertToMetricEntity(sentinelMetric));
        }

        Map<String, MetricEntity> resourceCount = new HashMap<>(32);

        for (MetricEntity metricEntity : metricEntities) {
            String resource = metricEntity.getResource();
            if (resourceCount.containsKey(resource)) {
                MetricEntity oldEntity = resourceCount.get(resource);
                oldEntity.addPassQps(metricEntity.getPassQps());
                oldEntity.addRtAndSuccessQps(metricEntity.getRt(), metricEntity.getSuccessQps());
                oldEntity.addBlockQps(metricEntity.getBlockQps());
                oldEntity.addExceptionQps(metricEntity.getExceptionQps());
                oldEntity.addCount(1);
            } else {
                resourceCount.put(resource, MetricEntity.copyOf(metricEntity));
            }
        }

        // Order by last minute b_qps DESC.
        return resourceCount.entrySet()
                .stream()
                .sorted((o1, o2) -> {
                    MetricEntity e1 = o1.getValue();
                    MetricEntity e2 = o2.getValue();
                    int t = e2.getBlockQps().compareTo(e1.getBlockQps());
                    if (t != 0) {
                        return t;
                    }
                    return e2.getPassQps().compareTo(e1.getPassQps());
                })
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
    }

    private MetricEntity convertToMetricEntity(SentinelMetric sentinelMetric) {
        MetricEntity metricEntity = new MetricEntity();

//        metricEntity.setId(sentinelMetric.getId());
        metricEntity.setGmtCreate(new Date(sentinelMetric.getGmtCreate()));
//        metricEntity.setGmtModified(new Date(sentinelMetric.getGmtModified()));
        metricEntity.setApp(sentinelMetric.getApp());
        metricEntity.setTimestamp(Date.from(sentinelMetric.getTime()));
        metricEntity.setResource(sentinelMetric.getResource());
        metricEntity.setPassQps(sentinelMetric.getPassQps());
        metricEntity.setSuccessQps(sentinelMetric.getSuccessQps());
        metricEntity.setBlockQps(sentinelMetric.getBlockQps());
        metricEntity.setExceptionQps(sentinelMetric.getExceptionQps());
        metricEntity.setRt(sentinelMetric.getRt());
        metricEntity.setCount(sentinelMetric.getCount());

        return metricEntity;
    }

    private void doSave(InfluxDB influxDB, MetricEntity metric) {
        influxDB.write(Point.measurement(METRIC_MEASUREMENT)
                .time(DateUtils.addHours(metric.getTimestamp(), 0).getTime(), TimeUnit.MILLISECONDS)
                .tag("app", metric.getApp())
                .tag("resource", metric.getResource())
//                .addField("id", metric.getId())
                .addField("gmtCreate", metric.getGmtCreate().getTime())
//                .addField("gmtModified", metric.getGmtModified().getTime())
                .addField("passQps", metric.getPassQps())
                .addField("successQps", metric.getSuccessQps())
                .addField("blockQps", metric.getBlockQps())
                .addField("exceptionQps", metric.getExceptionQps())
                .addField("rt", metric.getRt())
                .addField("count", metric.getCount())
                .addField("resourceCode", metric.getResourceCode())
                .build());
    }
}