package cli

type Segment struct {
	dbid          int
	contentId     int
	port          int
	dataDirectory string
	hostAddress   string
	hostName      string
}

type Locale struct {
	lc_all, lc_collate, lc_ctype, lc_messages, lc_monetory, lc_numeric, lc_time string
}

// Required only during creation of cluster
type ClusterParams struct {
	//coordinator config
	CoordinatorConfig []map[string]string
	//primary config
	SegmentConfig map[string]string
	// Common config
	CommonConfig map[string]string
	//cluster parameters
	Locale Locale
	// List of Coordinator IP Addresses
	CoordinatorIPlist []string

	// more parameters
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
