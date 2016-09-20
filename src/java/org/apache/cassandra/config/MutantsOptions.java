package org.apache.cassandra.config;

import org.apache.commons.lang3.builder.ToStringBuilder;

public class MutantsOptions
{
    public boolean migrate_sstables;

    public int sst_migration_decision_interval_in_ms;
    public double sst_tempmon_time_window_in_sec;
    public double sst_tempmon_threshold_num_per_sec;

    public String cold_storage_dir;

    public long tablet_access_stat_report_interval_in_ms;

    @Override
    public String toString()
    {
        return ToStringBuilder.reflectionToString(this);
    }
}
