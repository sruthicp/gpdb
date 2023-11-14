package cli

import (
	"fmt"
	"io"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpdb/gp/idl"
	"github.com/greenplum-db/gpdb/gp/utils"
	"github.com/vbauerster/mpb/v8"
)

type streamReceiver interface {
	Recv() (*idl.HubReply, error)
}

func ParseStreamResponse(stream streamReceiver) error {
	progressBarMap := make(map[string]*mpb.Bar)
	progressInstance := utils.NewProgressInstance()

	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			for _, bar := range progressBarMap {
				bar.Abort(false)
			}
			progressInstance.Wait()

			return utils.FormatGrpcError(err)
		}

		msg := resp.Message
		switch msg.(type) {
		case *idl.HubReply_LogMsg:
			if len(progressBarMap) != 0 {
				progressInstance.Wait()				
			}
			gplog.Info(resp.GetLogMsg())

		case *idl.HubReply_StdoutMsg:
			if len(progressBarMap) != 0 {
				progressInstance.Wait()				
			}
			fmt.Print(resp.GetStdoutMsg())

		case *idl.HubReply_ProgressMsg:
			progressMsg := resp.GetProgressMsg()
			if _, ok := progressBarMap[progressMsg.Label]; !ok {
				progressBarMap[progressMsg.Label] = utils.NewProgressBar(progressInstance, progressMsg.Label, int(progressMsg.Total))
			} else {
				progressBarMap[progressMsg.Label].Increment()
			}
		}
	}

	progressInstance.Wait()
	
	return nil
}
