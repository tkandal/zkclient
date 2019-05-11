package zkclient

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/tkandal/zookclient"
	"time"
)

/*
 * Copyright (c) 2019 Norwegian University of Science and Technology
 */

const (
	// StatusSuccess is the value for success in StateInfo
	StatusSuccess = "OK"

	// StatusFail is the value for fail in StateInfo
	StatusFail = "FAILED"
)

// Properties is a sub-struct for NodeInfo
type Properties struct {
	Version string `json:"version"`
}

// NodeInfo contains information about this program in Zookeeper
type NodeInfo struct {
	Name          string     `json:"name"`
	LastStartTime int64      `json:"lastStartTime,omitempty"`
	LastExitTime  int64      `json:"lastExitTime,omitempty"`
	Properties    Properties `json:"properties,omitempty"`
}

// LiveNodeInfo is information that is pushed to Zookeeper to signal that it is alive
type LiveNodeInfo struct {
	Name      string `json:"name"`
	Host      string `json:"host,omitempty"`
	StartTime int64  `json:"startTime,omitempty"`
	StatusURL string `json:"statusUrl,omitempty"`
}

// ZKStateInfo contains state-information that is pushed to Zookeeper
type ZKStateInfo struct {
	Name      string `json:"name"`
	Status    string `json:"status,omitempty"`
	StartTime int64  `json:"startTime,omitempty"`
	EndTime   int64  `json:"endTime,omitempty"`
	Error     string `json:"error,omitempty"`
}

// StateInfoElement contains the detailed information about this program
type StateInfoElement struct {
	Status        string   `json:"status,omitempty"`
	SourceSystems []string `json:"sourceSystems,omitempty"`
	StartTime     int64    `json:"startTime,omitempty"`
	LastUpdated   int64    `json:"lastUpdated,omitempty"`
	EndTime       int64    `json:"endTime,omitempty"`
	Error         string   `json:"error,omitempty"`
}

// StateInfo contains state-information about this program, which is also dumped on web
type StateInfo map[string]StateInfoElement

// Implements the Stringer interface, and dumps as JSON
func (si StateInfo) String() string {
	buf := bytes.NewBuffer(make([]byte, 0))
	if err := json.NewEncoder(buf).Encode(si); err != nil {
		return ""
	}
	return buf.String()
}

var (
	// GitTag is the commit-hash from GIT that was used when building this program
	GitTag string
	// Builder is the username that built this program
	Builder string
	// BuiltAt is the timestamp when this program was built
	BuiltAt string
)

// ZooKeeper defines the interface for a zookeeper-client
type ZooKeeper interface {
	Close() error
	Dial(string) error
	SetState(ZKStateInfo) error
	ReadState() (*ZKStateInfo, error)
}

type zooKeeper struct {
	zooHost   string
	zkClient  *zookclient.ZooKeeperClient
	nodeInfo  NodeInfo
	livePath  string
	infoPath  string
	statePath string
	name      string
	stateHost string
	statePort string
}

func (zk *zooKeeper) Dial(host string) error {
	zkClient, err := zookclient.NewZooKeeperClient(host)
	if err != nil {
		return fmt.Errorf("connect %s failed; error = %v", zk.zooHost, err)
	}
	zk.zkClient = zkClient
	zk.zooHost = host

	if !zkClient.Exists(zk.livePath) {
		if err := zkClient.CreatePath(zk.livePath); err != nil {
			_ = zk.zkClient.Close()
			return fmt.Errorf("create path %s%s failed; error = %v", zk.zooHost, zk.livePath, err)
		}
	}
	if !zkClient.Exists(zk.infoPath) {
		if err = zkClient.CreatePath(zk.infoPath); err != nil {
			_ = zk.zkClient.Close()
			return fmt.Errorf("create path %s%s failed; error = %v", zk.zooHost, zk.infoPath, err)
		}
	}
	if !zkClient.Exists(zk.statePath) {
		if err = zkClient.CreatePath(zk.statePath); err != nil {
			_ = zk.zkClient.Close()
			return fmt.Errorf("create path %s%s failed; error = %v", zk.zooHost, zk.statePath, err)
		}
	}

	startTime := time.Now().UnixNano() / int64(time.Millisecond)
	liveNodeInfo := LiveNodeInfo{
		Name:      zk.name,
		Host:      zk.stateHost,
		StartTime: startTime,
		StatusURL: fmt.Sprintf("http://%s:%s", zk.stateHost, zk.statePort),
	}
	content, err := toBytes(liveNodeInfo)
	if err != nil {
		_ = zk.zkClient.Close()
		return fmt.Errorf("encode live-node %s:%s failed; error = %v", zk.zooHost, zk.livePath, err)
	}
	if err = zkClient.CreateEphemeralNode(zk.livePath, content); err != nil {
		_ = zk.zkClient.Close()
		return fmt.Errorf("set live-node %s%s failed; error = %v", zk.zooHost, zk.livePath, err)
	}

	zk.nodeInfo = NodeInfo{
		Name:          zk.name,
		LastStartTime: startTime,
		Properties:    Properties{Version: fmt.Sprintf("Built=%s, GitTag=%s, Builder=%s", BuiltAt, GitTag, Builder)},
	}
	content, err = toBytes(zk.nodeInfo)
	if err != nil {
		_ = zk.zkClient.Close()
		return fmt.Errorf("encode node-info %s%s failed; error = %v", zk.zooHost, zk.infoPath, err)
	}
	if err = zkClient.CreateNode(zk.infoPath, content); err != nil {
		return fmt.Errorf("set node-info %s%s failed; error = %v", zk.zooHost, zk.infoPath, err)
	}

	return nil
}

func (zk *zooKeeper) SetState(stateInfo ZKStateInfo) error {
	content, err := toBytes(stateInfo)
	if err != nil {
		return fmt.Errorf("encode state-node %s%s failed; error = %v", zk.zooHost, zk.statePath, err)
	}
	if err := zk.zkClient.CreateEphemeralNode(zk.statePath, content); err != nil {
		return fmt.Errorf("set state-node %s%s failed; error = %v", zk.zooHost, zk.statePath, err)
	}
	return nil
}

func (zk *zooKeeper) ReadState() (*ZKStateInfo, error) {
	if !zk.zkClient.Exists(zk.statePath) {
		return nil, fmt.Errorf("path %s%s does not exist", zk.zooHost, zk.statePath)
	}
	content, err := zk.zkClient.GetData(zk.statePath)
	if err != nil {
		return nil, fmt.Errorf("get data from %s%s failed; error = %v", zk.zooHost, zk.statePath, err)
	}
	if content == nil {
		return nil, nil
	}

	buf := bytes.NewBuffer(content)
	zkStateInfo := ZKStateInfo{}
	if err := json.NewDecoder(buf).Decode(&zkStateInfo); err != nil {
		return nil, fmt.Errorf("json decode state-info %s%s failed; error = %v", zk.zooHost, zk.statePath, err)
	}
	return &zkStateInfo, nil
}

func (zk *zooKeeper) Close() error {
	zk.nodeInfo.LastExitTime = time.Now().UnixNano() / int64(time.Millisecond)
	content, err := toBytes(zk.nodeInfo)
	if err != nil {
		return fmt.Errorf("encode last exit time for %s%s failed; error = %v", zk.zooHost, zk.infoPath, err)
	}
	if err := zk.zkClient.SetByte(zk.infoPath, content); err != nil {
		return fmt.Errorf("set last exit time %s%s failed; error = %v", zk.zooHost, zk.infoPath, err)
	}
	if err := zk.zkClient.Close(); err != nil {
		return fmt.Errorf("close zookeeper %s failed; error = %v", zk.zooHost, err)
	}
	return nil
}

func toBytes(obj interface{}) ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0))
	if err := json.NewEncoder(buf).Encode(obj); err != nil {
		return nil, fmt.Errorf("encode to json failed; error = %v", err)
	}
	return buf.Bytes(), nil
}
