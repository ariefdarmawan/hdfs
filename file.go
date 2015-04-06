package hdfs

import (
	"errors"
	//"io/ioutil"
	//"os"
	"path/filepath"
	"strconv"
)

func (h *Hdfs) Get(path string) error {
	return nil
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
	//parms = mergeMapString(parms, map[string]string{"permission": strconv.Itoa(permission)})
	for _, path := range paths {
		_, filename := filepath.Split(path)
		newfilename := filepath.Join(destinationFolder, filename)
		e := h.Put(path, newfilename, permission, parms)
		if e != nil {
			if es == nil {
				es = make(map[string]error)
				es[path] = e
			}
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
