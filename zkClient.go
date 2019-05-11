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

type ZookeeperClient struct {
	zooHost   string
	zkClient  *zookclient.ZooKeeperClient
	nodeInfo  NodeInfo
	LivePath  string
	InfoPath  string
	StatePath string
	Name      string
	StateHost string
	StatePort string
}

func (zk *ZookeeperClient) Dial(host string) error {
	zkClient, err := zookclient.NewZooKeeperClient(host)
	if err != nil {
		return fmt.Errorf("connect %s failed; error = %v", zk.zooHost, err)
	}
	zk.zkClient = zkClient
	zk.zooHost = host

	if !zkClient.Exists(zk.LivePath) {
		if err := zkClient.CreatePath(zk.LivePath); err != nil {
			_ = zk.zkClient.Close()
			return fmt.Errorf("create path %s%s failed; error = %v", zk.zooHost, zk.LivePath, err)
		}
	}
	if !zkClient.Exists(zk.InfoPath) {
		if err = zkClient.CreatePath(zk.InfoPath); err != nil {
			_ = zk.zkClient.Close()
			return fmt.Errorf("create path %s%s failed; error = %v", zk.zooHost, zk.InfoPath, err)
		}
	}
	if !zkClient.Exists(zk.StatePath) {
		if err = zkClient.CreatePath(zk.StatePath); err != nil {
			_ = zk.zkClient.Close()
			return fmt.Errorf("create path %s%s failed; error = %v", zk.zooHost, zk.StatePath, err)
		}
	}

	startTime := time.Now().UnixNano() / int64(time.Millisecond)
	liveNodeInfo := LiveNodeInfo{
		Name:      zk.Name,
		Host:      zk.StateHost,
		StartTime: startTime,
		StatusURL: fmt.Sprintf("http://%s:%s", zk.StateHost, zk.StatePort),
	}
	content, err := toBytes(liveNodeInfo)
	if err != nil {
		_ = zk.zkClient.Close()
		return fmt.Errorf("encode live-node %s:%s failed; error = %v", zk.zooHost, zk.LivePath, err)
	}
	if err = zkClient.CreateEphemeralNode(zk.LivePath, content); err != nil {
		_ = zk.zkClient.Close()
		return fmt.Errorf("set live-node %s%s failed; error = %v", zk.zooHost, zk.LivePath, err)
	}

	zk.nodeInfo = NodeInfo{
		Name:          zk.Name,
		LastStartTime: startTime,
		Properties:    Properties{Version: fmt.Sprintf("Built=%s, GitTag=%s, Builder=%s", BuiltAt, GitTag, Builder)},
	}
	content, err = toBytes(zk.nodeInfo)
	if err != nil {
		_ = zk.zkClient.Close()
		return fmt.Errorf("encode node-info %s%s failed; error = %v", zk.zooHost, zk.InfoPath, err)
	}
	if err = zkClient.CreateNode(zk.InfoPath, content); err != nil {
		return fmt.Errorf("set node-info %s%s failed; error = %v", zk.zooHost, zk.InfoPath, err)
	}

	return nil
}

func (zk *ZookeeperClient) SetState(stateInfo ZKStateInfo) error {
	content, err := toBytes(stateInfo)
	if err != nil {
		return fmt.Errorf("encode state-node %s%s failed; error = %v", zk.zooHost, zk.StatePath, err)
	}
	if err := zk.zkClient.CreateEphemeralNode(zk.StatePath, content); err != nil {
		return fmt.Errorf("set state-node %s%s failed; error = %v", zk.zooHost, zk.StatePath, err)
	}
	return nil
}

func (zk *ZookeeperClient) ReadState() (*ZKStateInfo, error) {
	if !zk.zkClient.Exists(zk.StatePath) {
		return nil, fmt.Errorf("path %s%s does not exist", zk.zooHost, zk.StatePath)
	}
	content, err := zk.zkClient.GetData(zk.StatePath)
	if err != nil {
		return nil, fmt.Errorf("get data from %s%s failed; error = %v", zk.zooHost, zk.StatePath, err)
	}
	if content == nil {
		return nil, nil
	}

	buf := bytes.NewBuffer(content)
	zkStateInfo := ZKStateInfo{}
	if err := json.NewDecoder(buf).Decode(&zkStateInfo); err != nil {
		return nil, fmt.Errorf("json decode state-info %s%s failed; error = %v", zk.zooHost, zk.StatePath, err)
	}
	return &zkStateInfo, nil
}

func (zk *ZookeeperClient) Close() error {
	zk.nodeInfo.LastExitTime = time.Now().UnixNano() / int64(time.Millisecond)
	content, err := toBytes(zk.nodeInfo)
	if err != nil {
		return fmt.Errorf("encode last exit time for %s%s failed; error = %v", zk.zooHost, zk.InfoPath, err)
	}
	if err := zk.zkClient.SetByte(zk.InfoPath, content); err != nil {
		return fmt.Errorf("set last exit time %s%s failed; error = %v", zk.zooHost, zk.InfoPath, err)
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
