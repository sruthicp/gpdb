package agent

import (
	"context"
	"fmt"
	"strconv"

	"golang.org/x/exp/maps"

	"github.com/greenplum-db/gpdb/gp/idl"
	"github.com/greenplum-db/gpdb/gp/utils"
	"github.com/greenplum-db/gpdb/gp/utils/postgres"
)

var (
	UpdatePostgresqlConf       = postgres.UpdatePostgresqlConf
	CreatePostgresInternalConf = postgres.CreatePostgresInternalConf
	UpdateCoordinatorPgHbaConf = postgres.UpdateCoordinatorPgHbaConf
	UpdateSegmentPgHbaConf     = postgres.UpdateSegmentPgHbaConf
)

func (s *Server) MakeSegment(ctx context.Context, in *idl.MakeSegmentRequest) (*idl.MakeSegmentReply, error) {
	dataDirectory := in.Segment.DataDirectory
	locale := in.Locale

	initdbOptions := postgres.Initdb{
		PgData:        dataDirectory,
		Encoding:      in.Encoding,
		LcCollate:     locale.LcCollate,
		LcCtype:       locale.LcCtype,
		LcMessages:    locale.LcMessages,
		LcMonetory:    locale.LcMonetory,
		LcNumeric:     locale.LcNumeric,
		LcTime:        locale.LcTime,
		DataChecksums: true,
	}
	out, err := utils.RunExecCommand(&initdbOptions, s.GpHome)
	if err != nil {
		return &idl.MakeSegmentReply{}, fmt.Errorf("executing initdb: %s, %w", out, err)
	}

	configParams := make(map[string]string)
	maps.Copy(configParams, in.SegConfig)
	configParams["port"] = strconv.Itoa(int(in.Segment.Port))
	configParams["listen_addresses"] = "*"
	configParams["gp_contentid"] = strconv.Itoa(int(in.Segment.Contentid))
	if in.Segment.Contentid == -1 {
		configParams["log_statement"] = "all"
	}

	err = UpdatePostgresqlConf(dataDirectory, configParams, false)
	if err != nil {
		return &idl.MakeSegmentReply{}, fmt.Errorf("updating postgresql.conf: %w", err)
	}

	err = CreatePostgresInternalConf(dataDirectory, int(in.Segment.Dbid))
	if err != nil {
		return &idl.MakeSegmentReply{}, fmt.Errorf("creating internal.auto.conf: %w", err)
	}

	if in.Segment.Contentid == -1 {
		err = UpdateCoordinatorPgHbaConf(dataDirectory, in.HbaHostNames, in.Segment.HostName)
	} else {
		err = UpdateSegmentPgHbaConf(dataDirectory, in.HbaHostNames, in.IPList, in.Segment.HostName)
	}
	if err != nil {
		return &idl.MakeSegmentReply{}, fmt.Errorf("updating pg_hba.conf: %w", err)
	}

	return &idl.MakeSegmentReply{}, nil
}
