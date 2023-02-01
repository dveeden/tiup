// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package instance

import (
	"context"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	tiupexec "github.com/pingcap/tiup/pkg/exec"
	"github.com/pingcap/tiup/pkg/utils"
)

// DM represent a DM (Data Migration) instance.
type DM struct {
	instance
	pds []*PDInstance
	Process
}

var _ Instance = &DM{}

// NewDM create a DM instance.
func NewDM(binPath string, dir, host, configPath string, id int, pds []*PDInstance) *DM {
	dm := &DM{
		instance: instance{
			BinPath:    binPath,
			ID:         id,
			Dir:        dir,
			Host:       host,
			Port:       utils.MustGetFreePort(host, 8249),
			ConfigPath: configPath,
		},
		pds: pds,
	}
	dm.StatusPort = dm.Port
	return dm
}

// NodeID return the node id of DM.
func (p *DM) NodeID() string {
	return fmt.Sprintf("dm_%d", p.ID)
}

// Ready return nil when DM is ready to serve.
func (p *DM) Ready(ctx context.Context) error {
	url := fmt.Sprintf("http://%s/status", utils.JoinHostPort(p.Host, p.Port))

	ready := func() bool {
		resp, err := http.Get(url)
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		return resp.StatusCode == 200
	}

	for {
		if ready() {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
			// just retry
		}
	}
}

// Addr return the address of DM.
func (p *DM) Addr() string {
	return utils.JoinHostPort(AdvertiseHost(p.Host), p.Port)
}

// Start implements Instance interface.
func (p *DM) Start(ctx context.Context, version utils.Version) error {
	endpoints := pdEndpoints(p.pds, true)

	args := []string{
		fmt.Sprintf("--node-id=%s", p.NodeID()),
		fmt.Sprintf("--addr=%s", utils.JoinHostPort(p.Host, p.Port)),
		fmt.Sprintf("--advertise-addr=%s", utils.JoinHostPort(AdvertiseHost(p.Host), p.Port)),
		fmt.Sprintf("--pd-urls=%s", strings.Join(endpoints, ",")),
		fmt.Sprintf("--log-file=%s", p.LogFile()),
	}
	if p.ConfigPath != "" {
		args = append(args, fmt.Sprintf("--config=%s", p.ConfigPath))
	}

	var err error
	if p.BinPath, err = tiupexec.PrepareBinary("dm", version, p.BinPath); err != nil {
		return err
	}
	p.Process = &process{cmd: PrepareCommand(ctx, p.BinPath, args, nil, p.Dir)}

	logIfErr(p.Process.SetOutputFile(p.LogFile()))
	return p.Process.Start()
}

// Component return component name.
func (p *DM) Component() string {
	return "pump"
}

// LogFile return the log file.
func (p *DM) LogFile() string {
	return filepath.Join(p.Dir, "dm.log")
}
