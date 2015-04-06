// Provide HDFS WebApi wrapper to connect to Hadoop cluster. This package was written on 2.5.1 but should be work on other version that has same signature
package hdfs

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"os/user"
	"strings"
	"time"
)

// Constant
const WebHdfsApi string = "/webhdfs/v1/"
const (
	OP_OPEN                  = "OPEN"
	OP_CREATE                = "CREATE"
	OP_APPEND                = "APPEND"
	OP_CONCAT                = "CONCAT"
	OP_RENAME                = "RENAME"
	OP_DELETE                = "DELETE"
	OP_SETPERMISSION         = "SETPERMISSION"
	OP_SETOWNER              = "SETOWNER"
	OP_SETREPLICATION        = "SETREPLICATION"
	OP_SETTIMES              = "SETTIMES"
	OP_MKDIRS                = "MKDIRS"
	OP_CREATESYMLINK         = "CREATESYMLINK"
	OP_LISTSTATUS            = "LISTSTATUS"
	OP_GETFILESTATUS         = "GETFILESTATUS"
	OP_GETCONTENTSUMMARY     = "GETCONTENTSUMMARY"
	OP_GETFILECHECKSUM       = "GETFILECHECKSUM"
	OP_GETDELEGATIONTOKEN    = "GETDELEGATIONTOKEN"
	OP_GETDELEGATIONTOKENS   = "GETDELEGATIONTOKENS"
	OP_RENEWDELEGATIONTOKEN  = "RENEWDELEGATIONTOKEN"
	OP_CANCELDELEGATIONTOKEN = "CANCELDELEGATIONTOKEN"
)

type HdfsConfig struct {
	Host    string
	UserId  string
	TimeOut time.Duration
}

type Hdfs struct {
	Config *HdfsConfig
	client *http.Client
}

func NewHdfsConfig(host, userid string) *HdfsConfig {
	cfg := HdfsConfig{}
	cfg.TimeOut = time.Second * 15
	cfg.Host = host

	if userid == "" {
		user, err := user.Current()
		if err == nil {
			userid = user.Username
		}
	}
	cfg.UserId = userid
	return &cfg
}

func NewHdfs(config *HdfsConfig) (*Hdfs, error) {
	hdfs := new(Hdfs)
	hdfs.Config = config

	hdfs.client = &http.Client{
		Transport: &http.Transport{
			Dial: func(netw, addr string) (net.Conn, error) {
				c, err := net.DialTimeout(netw, addr, config.TimeOut)
				if err != nil {
					return nil, err
				}
				return c, nil
			},
		},
	}
	return hdfs, nil
}

func (h *Hdfs) makePath(path string, op string, parms map[string]string) string {
	s := h.Config.Host
	s = s + WebHdfsApi
	if path[0] == '/' {
		path = path[1:]
	}
	s = s + path
	if op != "" {
		s = s + "?op=" + op
	}
	s = s + "&user.name=" + h.Config.UserId
	for k, v := range parms {
		s += "&" + k + "=" + v
	}
	return s
}

func (h *Hdfs) call(calltype, path, op string, parms map[string]string) (*http.Response, error) {
	url := ""
	if strings.HasPrefix(path, "http") == false {
		url = h.makePath(path, op, parms)
	} else {
		url = path
	}
	//return nil, errors.New(url)

	req, err := http.NewRequest(calltype, url, nil)
	if err != nil {
		return nil, err
	}
	return h.client.Do(req)
}

func (h *Hdfs) callPayload(calltype, path, op string, filename string, parms map[string]string) (*http.Response, error) {
	url := ""
	if strings.HasPrefix(path, "http") == false {
		url = h.makePath(path, op, parms)
	} else {
		url = path
	}

	payload, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer payload.Close()

	req, err := http.NewRequest(calltype, url, payload)
	if err != nil {
		return nil, err
	}
	return h.client.Do(req)
}

func handleRespond(r *http.Response) (*HdfsData, error) {
	hdata := new(HdfsData)
	data, e := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	if e != nil {
		return hdata, e
	}
	e = json.Unmarshal(data, hdata)
	if e != nil {
		return hdata, e
	}
	if hdata.RemoteException.Message != "" {
		return hdata, errors.New(hdata.RemoteException.Message)
	}
	return hdata, nil
}
