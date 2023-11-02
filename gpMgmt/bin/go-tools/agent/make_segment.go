package agent

import (
	"context"
	"fmt"
	"strconv"

	"golang.org/x/exp/maps"

	"github.com/greenplum-db/gpdb/gp/idl"
	"github.com/greenplum-db/gpdb/gp/utils/postgres"
)

func (s *Server) MakeSegment(ctx context.Context, in *idl.MakeSegmentRequest) (*idl.MakeSegmentReply, error) {
	dataDirectory := in.Segment.GetDataDirectory()
	locale := in.GetLocale()

	initdbOptions := postgres.Initdb{
		PgData: dataDirectory,
		Encoding: in.GetEncoding(),
		LcCollate: locale.GetLcCollate(),
		LcCtype: locale.GetLcCtype(),
		DataChecksums: true,
	}
	out, err := postgres.RunPgCommand(&initdbOptions, s.GpHome)
	if err != nil {
		return &idl.MakeSegmentReply{}, fmt.Errorf("executing initdb: %s, %w", out, err)
	}

	configParams := make(map[string]string)
	maps.Copy(configParams, in.SegConfig)
	configParams["port"] = strconv.Itoa(int(in.Segment.Port))
	configParams["listen_addresses"] = "*"
	configParams["log_statement"] = "all"
	configParams["gp_contentid"] = strconv.Itoa(int(in.Segment.Contentid))

	err = postgres.UpdatePostgresqlConf(dataDirectory, configParams, false)
	if err != nil {
		return &idl.MakeSegmentReply{}, fmt.Errorf("updating postgresql.conf: %w", err)
	}

	err = postgres.CreatePostgresInternalConf(dataDirectory, int(in.Segment.GetDbid()))
	if err != nil {
		return &idl.MakeSegmentReply{}, fmt.Errorf("creating internal.auto.conf: %w", err)
	}

	if in.Segment.Contentid == -1 {
		err = postgres.UpdateCoordinatorPgHbaConf(dataDirectory, in.HbaHostNames, in.Segment.HostName)
	} else {
		err = postgres.UpdateSegmentPgHbaConf(dataDirectory, in.HbaHostNames, in.IPList, in.Segment.HostName)
	}
	if err != nil {
		return &idl.MakeSegmentReply{}, fmt.Errorf("updating pg_hba.conf: %w", err)
	}

	return &idl.MakeSegmentReply{}, nil
}
