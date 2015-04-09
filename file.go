package hdfs

import (
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

func (h *Hdfs) GetToLocal(path string, destination string, permission int) error {
	d, err := h.Get(path)
	if err != nil {
		return err
	}
	if permission == 0 {
		permission = 766
	}
	err = ioutil.WriteFile(destination, d, os.FileMode(permission))
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

func (h *Hdfs) Put(localfile string, destination string, permission int, parms map[string]string) error {
	if permission == 0 {
		permission = 766
	}
	parms = mergeMapString(parms, map[string]string{"permission": strconv.Itoa(permission)})
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

func (h *Hdfs) Puts(paths []string, destinationFolder string, permission int, parms map[string]string) map[string]error {
	var es map[string]error
	if permission == 0 {
		permission = 766
	}

	wg := sync.WaitGroup{}
	wg.Add(len(paths))

	//parms = mergeMapString(parms, map[string]string{"permission": strconv.Itoa(permission)})
	for _, path := range paths {
		go func(swg *sync.WaitGroup) {
			defer swg.Done()
			_, filename := filepath.Split(path)
			newfilename := filepath.Join(destinationFolder, filename)
			e := h.Put(path, newfilename, permission, parms)
			if e != nil {
				if es == nil {
					es = make(map[string]error)
					es[path] = e
				}
			}
		}(&wg)
	}
	wg.Wait()
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
