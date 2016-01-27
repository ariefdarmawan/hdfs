package hdfs

import (
	"fmt"
	"testing"
)

func TestHdfs(t *testing.T) {
	h, e := NewHdfs(NewHdfsConfig("http://awshdc01:50070", "hdfs"))
	if e != nil {
		t.Error(e.Error())
	}
	h.Config.PoolSize = 100

	fmt.Println(">>>> TEST DELETE FOLDERS <<<<")
	if e = h.Delete("/user/ariefdarmawan", true); e != nil {
		t.Error(e.Error())
	}

	fmt.Println(">>>> TEST CHANGE OWNER <<<<")
	if e = h.SetOwner("/user/ariefdarmawan", "ariefdarmawan", ""); e != nil {
		t.Error(e.Error())
	}

	fmt.Println(">>>> TEST CREATE DIR <<<<")
	es := h.MakeDirs([]string{"/user/ariefdarmawan/inbox", "/user/ariefdarmawan/temp", "/user/ariefdarmawan/outbox"}, "")
	if es != nil {
		for k, v := range es {
			t.Error(fmt.Sprintf("Error when create %v : %v \n", k, v))
		}
	}

	/*
		fmt.Println(">>>> TEST COPY DIR <<<<")
		e, es = h.PutDir("/Users/ariefdarmawan/Temp/ECFZ/TempVisa/JSON", "/user/ariefdarmawan/inbox/ecfz/json")
		if es != nil {
			for k, v := range es {
				t.Error(fmt.Sprintf("Error when create %v : %v \n", k, v))
			}
		}
	*/

	fmt.Println(">>>> TEST PUT FILE <<<<")
	e = h.Put("/Users/ariefdarmawan/Temp/BHPWellReport.pdf", "/user/ariefdarmawan/inbox/wellreport.pdf", "", nil)
	if e != nil {
		t.Error(e.Error())
	}

	fmt.Println(">>>> TEST GET STATUS <<<<")
	hdata, e := h.List("/user/ariefdarmawan")
	if e != nil {
		t.Error(e.Error())
	} else {
		fmt.Printf("Data Processed :\n%v\n", len(hdata.FileStatuses.FileStatus))
	}

	fmt.Println("Test Done\n")
}
