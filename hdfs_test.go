package hdfs

import (
	"fmt"
	"testing"
)

func TestHdfs(t *testing.T) {
	h, e := NewHdfs(NewHdfsConfig("http://localhost:50070", ""))
	if e != nil {
		t.Error(e.Error())
	}

	fmt.Println(">>>> TEST DELETE FOLDERS <<<<")
	if e = h.Delete("/user", true); e != nil {
		t.Error(e.Error())
	}

	fmt.Println(">>>> TEST CREATE DIR <<<<")
	es := h.MakeDirs([]string{"/user/ariefdarmawan/inbox", "/user/ariefdarmawan/inbox/temp", "user/ariefdarmawan/outbox", "user/ariefdarmawan/done"}, 0)
	if es != nil {
		for k, v := range es {
			t.Error(fmt.Sprintf("Error when create %v : %v \n", k, v))
		}
	}

	fmt.Println(">>>> TEST PUT FILES <<<<")
	es = h.Puts([]string{
		"/users/ariefdarmawan/Temp/config.json",
		"/users/ariefdarmawan/Temp/ecis_test.js",
	}, "/user/ariefdarmawan/inbox/temp/", 0, nil)
	if es != nil {
		for k, v := range es {
			t.Error(fmt.Sprintf("Error when write %v : %v \n", k, v))
		}
	}

	fmt.Println(">>>> TEST GET STATUS <<<<")
	hdata, e := h.List("/user/ariefdarmawan")
	if e != nil {
		t.Error(e.Error())
	} else {
		fmt.Printf("Data:\n%v\n", hdata.FileStatuses.FileStatus)
	}

	fmt.Println("Test Done\n")
}
