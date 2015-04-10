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
	h.Config.PoolSize = 100

	fmt.Println(">>>> TEST DELETE FOLDERS <<<<")
	if e = h.Delete("/user", true); e != nil {
		t.Error(e.Error())
	}

	fmt.Println(">>>> TEST CREATE DIR <<<<")
	es := h.MakeDirs([]string{"/user/ariefdarmawan/inbox/ecfz/json", "/user/ariefdarmawan/inbox/temp"}, "")
	if es != nil {
		for k, v := range es {
			t.Error(fmt.Sprintf("Error when create %v : %v \n", k, v))
		}
	}

	fmt.Println(">>>> TEST COPY DIR <<<<")
	e, es = h.PutDir("/Users/ariefdarmawan/Temp/ECFZ/TempVisa/JSON", "/user/ariefdarmawan/inbox/ecfz/json")
	if es != nil {
		for k, v := range es {
			t.Error(fmt.Sprintf("Error when create %v : %v \n", k, v))
		}
	}

	/*
		fmt.Println(">>>> TEST PUT FILE <<<<")
		e = h.Put("/Users/ariefdarmawan/Temp/config.json", "/user/ariefdarmawan/inbox/temp/test.json", "", nil)
		if e != nil {
			t.Error(e.Error())
		}

		fmt.Println(">>>> TEST PUT FILES <<<<")
		es = h.Puts([]string{
			"/Users/ariefdarmawan/Temp/config.json",
			"/Users/ariefdarmawan/Temp/ecis_test.js",
		}, "/user/ariefdarmawan/inbox/temp/", "", nil)
		if es != nil {
			for k, v := range es {
				t.Error(fmt.Sprintf("Error when write %v : %v \n", k, v))
			}
		}
	*/

	fmt.Println(">>>> TEST GET STATUS <<<<")
	hdata, e := h.List("/user/ariefdarmawan/inbox/ecfz/json")
	if e != nil {
		t.Error(e.Error())
	} else {
		fmt.Printf("Data Processed :\n%v\n", len(hdata.FileStatuses.FileStatus))
	}

	fmt.Println("Test Done\n")
}
