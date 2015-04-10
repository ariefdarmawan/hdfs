package hdfs

import (
	"errors"
	//"fmt"
	"io/ioutil"
	//"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

func (h *Hdfs) GetToLocal(path string, destination string, permission string) error {
	d, err := h.Get(path)
	if err != nil {
		return err
	}
	if permission == "" {
		permission = "755"
	}
	iperm, _ := strconv.Atoi(permission)
	err = ioutil.WriteFile(destination, d, os.FileMode(iperm))
	if err != nil {
		return err
	}
	return nil
}

func (h *Hdfs) Get(path string) ([]byte, error) {
	r, err := h.call("GET", path, OP_OPEN, nil)
	if err != nil {
		return nil, err
	}
	if r.StatusCode != 307 {
		return nil, errors.New("Invalid Response Header on OP_OPEN")
	}

	location := r.Header["Location"][0]
	r, err = h.call("GET", location, OP_OPEN, nil)
	if err != nil {
		return nil, err
	}
	if r.StatusCode != 200 {
		return nil, errors.New(r.Status)
	}
	d, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()
	return d, nil
}

func mergeMapString(source map[string]string, adds map[string]string) map[string]string {
	if source == nil {
		source = make(map[string]string)
	}
	if adds != nil {
		for k, v := range adds {
			source[k] = v
		}
	}
	return source
}

func (h *Hdfs) Put(localfile string, destination string, permission string, parms map[string]string) error {
	if permission == "" {
		permission = "755"
	}
	parms = mergeMapString(parms, map[string]string{"permission": permission})
	r, err := h.call("PUT", destination, OP_CREATE, parms)
	if err != nil {
		return err
	}
	if r.StatusCode != 307 {
		return errors.New("Invalid Response Header on OP_CREATE")
	}

	location := r.Header["Location"][0]

	r, err = h.callPayload("PUT", location, OP_CREATE, localfile, nil)
	if err != nil {
		return err
	}
	if r.StatusCode != 201 {
		return errors.New(r.Status)
	}
	return nil
}

func (h *Hdfs) Puts(paths []string, destinationFolder string, permission string, parms map[string]string) map[string]error {
	var es map[string]error
	if permission == "" {
		permission = "755"
	}

	fileCount := len(paths)

	//parms = mergeMapString(parms, map[string]string{"permission": strconv.Itoa(permission)})
	ipool := 0
	iprocessing := 0
	iread := 0
	files := []string{}
	for _, path := range paths {
		ipool = ipool + 1
		iread = iread + 1
		files = append(files, path)
		if ipool == h.Config.PoolSize || iread == fileCount {
			wg := sync.WaitGroup{}
			wg.Add(ipool)

			for _, f := range files {
				go func(path string, swg *sync.WaitGroup) {
					defer swg.Done()
					iprocessing = iprocessing + 1
					_, filename := filepath.Split(path)
					newfilename := filepath.Join(destinationFolder, filename)
					e := h.Put(path, newfilename, permission, parms)
					//var e error
					if e != nil {
						if es == nil {
							es = make(map[string]error)
							es[path] = e
						}
						//fmt.Println(path, "=> ", newfilename, " ... FAIL => ", e.Error(), " | Processing ", iprocessing, " of ", fileCount)
					} else {
						//fmt.Println(path, "=> ", newfilename, " ... SUCCESS | Processing ", iprocessing, " of ", fileCount)
					}
				}(f, &wg)
			}

			wg.Wait()
			ipool = 0
			files = []string{}
		}
	}

	return es
}

func (h *Hdfs) Append(localfile string, destination string) error {
	r, err := h.call("PUT", destination, OP_APPEND, nil)
	if err != nil {
		return err
	}
	if r.StatusCode != 307 {
		return errors.New("Invalid Response Header on OP_APPEND")
	}

	location := r.Header["Location"][0]

	r, err = h.callPayload("PUT", location, OP_APPEND, localfile, nil)
	if err != nil {
		return err
	}
	if r.StatusCode != 201 {
		return errors.New(r.Status)
	}
	return nil
}

func (h *Hdfs) SetOwner(path string, user string) error {
	return nil
}

func (h *Hdfs) SetPermission(path string, user string) error {
	return nil
}
