package zkclient

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"github.com/tkandal/zookclient"
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
	Name          string   `json:"name,omitempty"`
	Status        string   `json:"status,omitempty"`
	StartTime     int64    `json:"startTime,omitempty"`
	EndTime       int64    `json:"endTime,omitempty"`
	LastUpdated   int64    `json:"lastUpdated"`
	SourceSystems []string `json:"sourceSystems,omitempty"`
	Error         string   `json:"error,omitempty"`
}

// StateInfoElement contains the detailed information about this program
type StateInfoElement struct {
	Status        string   `json:"status,omitempty"`
	SourceSystems []string `json:"sourceSystems,omitempty"`
	StartTime     int64    `json:"startTime,omitempty"`
	LastUpdated   int64    `json:"lastUpdated"`
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

// ZooKeeper defines the interface for a zookeeper-client
type ZooKeeper interface {
	Close() error
	Dial(string) error
	SetState(ZKStateInfo) error
	ReadState() (*ZKStateInfo, error)
	SetServersetMember(string, ServersetMember) error
	ReadServersetMember(string) (*ServersetMember, error)
	SetNerveMember(string, NerveMember) error
	ReadNerveMember(string) (*NerveMember, error)
	GetData(string) ([]byte, error)
	GetLeafNodes(string) ([]string, error)
}

// ServersetMember is an instance of serversets-status in Zookeeper.
// Serversets are commonly used by Finagle and Aurora. https://github.com/twitter/finagle
// NB! The field Status should always be set to "ALIVE", and will be removed in the future.
type ServersetMember struct {
	ServiceEndpoint     ServersetEndpoint            `json:"serviceEndpoint"`
	AdditionalEndpoints map[string]ServersetEndpoint `json:"additionalEndpoints"`
	Status              string                       `json:"status"`
	Shard               int                          `json:"shard"`
}

type ServersetEndpoint struct {
	Host string `json:"host"`
	Port int    `json:"port"`
}

// NerveMember is an instance of nerve-status in Zookeeper.
// Nerve is used in AirBnB's Nerve. https://github.com/airbnb/nerve
type NerveMember struct {
	Host string `json:"host"`
	Port int    `json:"port"`
	Name string `json:"name"`
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
	GitTag    string
	Builder   string
	BuiltAt   string
	liveNode  LiveNodeInfo
}

// Dial connects to zookeeper
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
		StatusURL: fmt.Sprintf("http://%s", zk.StateHost),
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
	zk.liveNode = liveNodeInfo

	zk.nodeInfo = NodeInfo{
		Name:          zk.Name,
		LastStartTime: startTime,
		Properties:    Properties{Version: fmt.Sprintf("Built=%s, GitTag=%s, Builder=%s", zk.BuiltAt, zk.GitTag, zk.Builder)},
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

// SetState sets state in state-path
func (zk *ZookeeperClient) SetState(stateInfo ZKStateInfo) error {
	content, err := toBytes(stateInfo)
	if err != nil {
		return fmt.Errorf("encode state-node %s%s failed; error = %v", zk.zooHost, zk.StatePath, err)
	}
	if err = zk.zkClient.CreateEphemeralNode(zk.StatePath, content); err != nil {
		return fmt.Errorf("set state-node %s%s failed; error = %v", zk.zooHost, zk.StatePath, err)
	}

	// create live-node in case it is gone
	liveInfo, err := toBytes(zk.liveNode)
	if err != nil {
		return fmt.Errorf("encode live-node %s%s failed; error = %v", zk.zooHost, zk.LivePath, err)
	}
	if err = zk.zkClient.CreateEphemeralNode(zk.LivePath, liveInfo); err != nil {
		return fmt.Errorf("set live-node %s%s failed; error = %v", zk.zooHost, zk.LivePath, err)
	}
	return nil
}

// SetState reads state in state-path
func (zk *ZookeeperClient) ReadState() (*ZKStateInfo, error) {
	if !zk.zkClient.Exists(zk.StatePath) {
		return nil, fmt.Errorf("path %s%s does not exist", zk.zooHost, zk.StatePath)
	}
	content, err := zk.zkClient.GetData(zk.StatePath)
	if err != nil {
		return nil, fmt.Errorf("get data from %s%s failed; error = %v", zk.zooHost, zk.StatePath, err)
	}
	if content == nil || len(content) == 0 {
		return nil, nil
	}

	buf := bytes.NewBuffer(content)
	zkStateInfo := ZKStateInfo{}
	if err := json.NewDecoder(buf).Decode(&zkStateInfo); err != nil {
		return nil, fmt.Errorf("json decode state-info %s%s failed; error = %v", zk.zooHost, zk.StatePath, err)
	}
	return &zkStateInfo, nil
}

func (zk *ZookeeperClient) SetServersetMember(path string, ssm ServersetMember) error {
	// Only ALIVE is legal
	ssm.Status = "ALIVE"
	content, err := toBytes(ssm)
	if err != nil {
		return fmt.Errorf("encode serversetMember-node %s%s failed; error = %v", zk.zooHost, path, err)
	}
	if err = zk.zkClient.CreateEphemeralNode(path, content); err != nil {
		return fmt.Errorf("set serversetMember-node %s%s failed; error = %v", zk.zooHost, path, err)
	}
	return nil
}

func (zk *ZookeeperClient) ReadServersetMember(path string) (*ServersetMember, error) {
	if !zk.zkClient.Exists(path) {
		return nil, fmt.Errorf("path %s%s does not exist", zk.zooHost, path)
	}
	content, err := zk.zkClient.GetData(path)
	if err != nil {
		return nil, fmt.Errorf("get data from %s%s failed; error = %v", zk.zooHost, path, err)
	}
	if content == nil || len(content) == 0 {
		return nil, nil
	}
	buf := bytes.NewBuffer(content)
	ssm := &ServersetMember{}
	if err := json.NewDecoder(buf).Decode(ssm); err != nil {
		return nil, fmt.Errorf("json decode serversetMember-info %s%s failed; error = %v", zk.zooHost, path, err)
	}
	return ssm, nil
}

func (zk *ZookeeperClient) SetNerveMember(path string, nm NerveMember) error {
	content, err := toBytes(nm)
	if err != nil {
		return fmt.Errorf("encode nervemember-node %s%s failed; error = %v", zk.zooHost, path, err)
	}
	if err = zk.zkClient.CreateEphemeralNode(path, content); err != nil {
		return fmt.Errorf("set nervemember-node %s%s failed; error = %v", zk.zooHost, path, err)
	}
	return nil
}

func (zk *ZookeeperClient) ReadNerveMember(path string) (*NerveMember, error) {
	if !zk.zkClient.Exists(path) {
		return nil, fmt.Errorf("path %s%s does not exist", zk.zooHost, path)
	}
	content, err := zk.zkClient.GetData(path)
	if err != nil {
		return nil, fmt.Errorf("get data from %s%s failed; error = %v", zk.zooHost, path, err)
	}
	if content == nil || len(content) == 0 {
		return nil, nil
	}
	buf := bytes.NewBuffer(content)
	nm := &NerveMember{}
	if err := json.NewDecoder(buf).Decode(nm); err != nil {
		return nil, fmt.Errorf("json decode nerveMember-info %s%s failed; error = %v", zk.zooHost, path, err)
	}
	return nm, nil
}

// Close closes the connection and sets last exit-time.
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

// GetData returns the data at the given path.
func (zk *ZookeeperClient) GetData(path string) ([]byte, error) {
	return zk.zkClient.GetData(path)
}

// GetLeafNodes get the paths to all leaf nodes under path.
func (zk *ZookeeperClient) GetLeafNodes(root string) ([]string, error) {
	return zk.zkClient.GetChildren(root)
}

func toBytes(obj interface{}) ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0))
	if err := json.NewEncoder(buf).Encode(obj); err != nil {
		return nil, fmt.Errorf("encode to json failed; error = %v", err)
	}
	return buf.Bytes(), nil
}
