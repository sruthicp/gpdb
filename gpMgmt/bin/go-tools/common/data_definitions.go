package common

import "github.com/greenplum-db/gpdb/gp/idl"

type Segment struct {
	Dbid          int
	ContentId     int
	Port          int
	DataDirectory string
	HostAddress   string
	HostName      string
}

func (s *Segment) LoadFromIdl(input *idl.Segment) {
	s.HostName = input.HostName
	s.HostAddress = input.HostAddress
	s.DataDirectory = input.DataDirectory
	s.Port = int(input.Port)
}

type Locale struct {
	lc_all, lc_collate, lc_ctype, lc_messages, lc_monetory, lc_numeric, lc_time string
}

func (l *Locale) LoadFromIdl(input *idl.Locale) {
	l.lc_all = input.LcAll
	l.lc_ctype = input.LcCtype
	l.lc_collate = input.LcCollate
	l.lc_messages = input.LcMessages
	l.lc_monetory = input.LcMonetory
	l.lc_numeric = input.LcNumeric
	l.lc_time = input.LcTime
}

// Required only during creation of cluster
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
	hbaHostname bool
	encoding    string
	suPassword  string
}

func (c *ClusterParams) LoadFromIdl(input *idl.ClusterParams) {
	c.SegmentConfig = input.SegmentConfig
	c.CommonConfig = input.CommonConfig
	c.CoordinatorConfig = input.CoordinatorConfig
	c.encoding = input.Encoding
	c.Locale.LoadFromIdl(input.Locale)
	c.hbaHostname = input.HbaHostnames
	c.suPassword = input.SuPassword
}

type GpArray struct {
	PrimarySegments []Segment
	Coordinator     Segment
}

func (g *GpArray) LoadFromIdl(input *idl.GpArray) {
	g.Coordinator.LoadFromIdl(input.Coordinator)
	for _, seg := range input.Primaries {
		newSeg := new(Segment)
		newSeg.LoadFromIdl(seg)
		g.PrimarySegments = append(g.PrimarySegments, *newSeg)
	}
}

// Structure to be used for creating the segment:
type segmentParams struct {
	segConfig         map[string]string
	commonConfig      map[string]string
	CoordinatorIPList []string
	port              int
	dataDir           string
}
