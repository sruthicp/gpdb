package cli

type Segment struct {
	dbid          int
	contentId     int
	port          int    `mapstructure:"port"`
	dataDirectory string `mapstructure:"data-directory"`
	hostAddress   string `mapstructure:"address"`
	hostName      string `mapstructure:"hostname"`
}

type Locale struct {
	LcAll      string `mapstructure:"lc-all"`
	LcCollate  string `mapstructure:"lc-collate"`
	LcCtype    string `mapstructure:"lc-ctype"`
	LcMessages string `mapstructure:"lc-messages"`
	LcMonetary string `mapstructure:"lc-monetary"`
	LcNumeric  string `mapstructure:"lc-numeric"`
	LcTime     string `mapstructure:"lc-time"`
}

// ClusterParams Required only during creation of cluster
type ClusterParams struct {
	//coordinator config
	CoordinatorConfig map[string]string
	//primary config
	SegmentConfig map[string]string
	// Common config
	CommonConfig map[string]string
	//cluster parameters
	Locale Locale
	// List of Coordinator IP Addresses
	CoordinatorIPlist []string

	// more parameters
	dbname      string
	hbaHostname bool
	encoding    string
	suPassword  string
}

type gpArray struct {
	PrimarySegments []Segment
	Coordinator     Segment
}

// Structure to be used for creating the segment:
type segmentParams struct {
	segConfig         map[string]string
	commonConfig      map[string]string
	CoordinatorIPList []string
	port              int
	dataDir           string
}
