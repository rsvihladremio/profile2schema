package profile2schema;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DatasetProfile {

  private String sql;

  private String datasetPath;

  private long type;

  private String batchSchema;

  private boolean allowApproxStats;

  public String getSql() {
    return sql;
  }

  public void setSql(final String sql) {
    this.sql = sql;
  }

  public void setDatasetPath(final String datasetPath) {
    this.datasetPath = datasetPath;
  }

  public String getDatasetPath() {
    return this.datasetPath;
  }

  public void setType(final long type) {
    this.type = type;
  }

  public long getType() {
    return this.type;
  }

  public void setBatchSchema(final String batchSchema) {
    this.batchSchema = batchSchema;
  }

  public String getBatchSchema() {
    return this.batchSchema;
  }

  public void setAllowApproxStats(final boolean allowApproxStats) {
    this.allowApproxStats = allowApproxStats;
  }

  public boolean getAllowApproxStats() {
    return this.allowApproxStats;
  }
}
