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
	Lc_all, Lc_collate, Lc_ctype, Lc_messages, Lc_monetory, Lc_numeric, Lc_time string
}

func (l *Locale) LoadFromIdl(input *idl.Locale) {
	l.Lc_all = input.LcAll
	l.Lc_ctype = input.LcCtype
	l.Lc_collate = input.LcCollate
	l.Lc_messages = input.LcMessages
	l.Lc_monetory = input.LcMonetory
	l.Lc_numeric = input.LcNumeric
	l.Lc_time = input.LcTime
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
	HbaHostname bool
	Encoding    string
	SuPassword  string
	DbName      string
}

func (c *ClusterParams) LoadFromIdl(input *idl.ClusterParams) {
	c.SegmentConfig = input.SegmentConfig
	c.CommonConfig = input.CommonConfig
	c.CoordinatorConfig = input.CoordinatorConfig
	c.Encoding = input.Encoding
	c.Locale.LoadFromIdl(input.Locale)
	c.HbaHostname = input.HbaHostnames
	c.SuPassword = input.SuPassword
	c.DbName = input.DbName
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
