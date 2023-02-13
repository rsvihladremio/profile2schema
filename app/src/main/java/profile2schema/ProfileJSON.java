package profile2schema;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class ProfileJSON{
    private List<DatasetProfile> datasetProfile;

    public void setDatasetProfile(final List<DatasetProfile> datasetProfile) {
        this.datasetProfile = datasetProfile;
    }
    public List<DatasetProfile> getDatasetProfile() {
        return this.datasetProfile;
    }
}
