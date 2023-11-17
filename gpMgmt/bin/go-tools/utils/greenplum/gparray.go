package greenplum

import (
	"fmt"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gpdb/gp/constants"
	"github.com/greenplum-db/gpdb/gp/idl"
	"github.com/greenplum-db/gpdb/gp/utils"
)

type Segment struct {
	Dbid          int
	ContentId     int
	Role          string
	PrefRole      string
	Mode          string
	State         string
	Port          int
	DataDirectory string
	HostAddress   string
	HostName      string
}

func (seg *Segment) isSegmentPrimary() bool {
	return seg.ContentId >= 0 && ((seg.Role == constants.RolePrimary) || (seg.PrefRole == constants.RolePrimary))
}

type GpArray struct {
	Segments []Segment
}

func NewGpArray() *GpArray {
	return &GpArray{}
}

//func main() {
//
//	primarys := []Segment{
//		{
//			port:          8002,
//			dataDirectory: "/Users/ravoorsh/workspace/gpdb/gpAux/gpdemo/datadirs/dbfast8/demoDataDir8",
//			hostName:      "ravoorsh3MD6R.vmware.com",
//			hostAddress:   "ravoorsh3MD6R.vmware.com",
//		},
//		{
//			port:          8003,
//			dataDirectory: "/Users/ravoorsh/workspace/gpdb/gpAux/gpdemo/datadirs/dbfast9/demoDataDir9",
//			hostName:      "ravoorsh3MD6R.vmware.com",
//			hostAddress:   "ravoorsh3MD6R.vmware.com",
//		},
//		{
//			port:          8004,
//			dataDirectory: "/Users/ravoorsh/workspace/gpdb/gpAux/gpdemo/datadirs/dbfast10/demoDataDir10",
//			hostName:      "ravoorsh3MD6R.vmware.com",
//			hostAddress:   "ravoorsh3MD6R.vmware.com",
//		},
//	}
//
//	err = nil
//	err = Segment.RegisterSegments(primarys)
//	if err != nil {
//		fmt.Println("Unable to register primaries to Segment")
//	}
//
//	GPArray, err = Segment.ReadGpSegmentConfig()
//	if err != nil {
//		fmt.Println("Unable to get data from gp_segment_configuration")
//	}
//
//	fmt.Println(len(GPArray))
//	fmt.Println(GPArray)
//
//	gpPrimary, err := GetPrimarySegments(GPArray)
//	if err != nil {
//		fmt.Println("Unable to get data from gp_segment")
//	}
//
//	fmt.Println(gpPrimary)
//}

func (gpArray *GpArray) ReadGpSegmentConfig(host string, port int) error {
	// Returns the contents of gp_segment_configuration table
	user, _ := utils.System.CurrentUser()
	conn := dbconn.NewDBConn("postgres", user.Username, host, port)
	defer conn.Close()

	conerr := conn.Connect(1, true)
	if conerr != nil {
		fmt.Println("Connection failed")
		return conerr
	}

	query := "select dbid, content, role, preferred_role, mode, status, port, datadir, hostname, address " +
		"from pg_catalog.gp_segment_configuration  order by content asc, role desc;"
	rows, err := conn.Query(query)
	defer rows.Close()

	if err != nil {
		return err
	}

	result, err := buildGpArray(rows)
	if err != nil {
		return err
	}

	gpArray.Segments = result

	return nil
}

func buildGpArray(rows *sqlx.Rows) ([]Segment, error) {

	result := []Segment{}

	for rows.Next() {
		dest := Segment{}

		if rErr := rows.Scan(
			&dest.Dbid,
			&dest.ContentId,
			&dest.Role,
			&dest.PrefRole,
			&dest.Mode,
			&dest.State,
			&dest.Port,
			&dest.DataDirectory,
			&dest.HostName,
			&dest.HostAddress,
		); rErr != nil {
			return nil, rErr
		}

		result = append(result, dest)
	}
	return result, nil
}

func (gpArray *GpArray) GetPrimarySegments() ([]Segment, error) {

	var result []Segment

	for _, seg := range gpArray.Segments {
		if seg.isSegmentPrimary() {
			result = append(result, seg)
		}
	}
	return result, nil
}

func RegisterPrimaries(segs []*idl.Segment, host string, port int) error {
	user, _ := utils.System.CurrentUser()
	conn := dbconn.NewDBConn("template1", user.Username, host, port)
	defer conn.Close()

	conerr := conn.Connect(1, true)
	if conerr != nil {
		fmt.Println("Connection failed")
		return nil
	}

	addPrimaryQuery := "SELECT pg_catalog.gp_add_segment_primary( '%s', '%s', %d, '%s');"
	for _, seg := range segs {
		addSegmentQuery := fmt.Sprintf(addPrimaryQuery, seg.HostName, seg.HostAddress, seg.Port, seg.DataDirectory)
		fmt.Println(addSegmentQuery)

		_, err := conn.Exec(addSegmentQuery)
		if err != nil {
			return err
		}
	}

	// FIXME: gp_add_segment_primary() starts the content ID from 1,
	// so manually update the correct values for now.
	updateContentIdQuery := "SET allow_system_table_mods=true; UPDATE gp_segment_configuration SET content = content - 1 where content > 0;"
	_, err := conn.Exec(updateContentIdQuery)
	if err != nil {
		return err
	}

	return nil
}

func RegisterCoordinator(seg *idl.Segment) error {
	user, _ := utils.System.CurrentUser()
	conn := dbconn.NewDBConn("template1", user.Username, seg.HostName, int(seg.Port))
	defer conn.Close()

	conerr := conn.Connect(1, true)
	if conerr != nil {
		fmt.Println("Connection failed")
		return nil
	}

	addCoordinatorQuery := "SELECT pg_catalog.gp_add_segment(1::int2, -1::int2, 'p', 'p', 's', 'u', '%d', '%s', '%s', '%s')"
	_, err := conn.Exec(fmt.Sprintf(addCoordinatorQuery, int(seg.Port), seg.HostName, seg.HostAddress, seg.DataDirectory))
	if err != nil {
		return err
	}
	return nil
}
