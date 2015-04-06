# hdfs
Golang wrapper for WebHDFS client

### Usage
```
go get -u github.com/juragan360/hdfs
```

### Connect to HDFS
```
h, e := NewHdfs(NewHdfsConfig("http://localhost:50070", "hadoopuser")) 
h, e := NewHdfs(NewHdfsConfig("http://localhost:50070", ""))  //--- automatically pass username from OS Context

```

### Make a HDFS dir
```
es := h.MakeDirs([]string{"/user/ariefdarmawan/inbox", "/user/ariefdarmawan/inbox/temp", "user/ariefdarmawan/outbox", "user/ariefdarmawan/done"}, 0)
	
if es != nil {
	for k, v := range es {
		fmt.Printf("Error when create %v : %v \n", k, v)
	}
}
```

### Put single file
```
err = h.Put("/users/ariefdarmawan/Temp/config.json", "/user/ariefdarmawan/inbox/temp/config.json", 0, map[string]string{"overwrite": "true"})
```

### Put multiple files
```
fmt.Println(">>>> TEST PUT FILE<<<<")
es = h.Puts([]string{
	"/users/ariefdarmawan/Temp/config.json",
	"/users/ariefdarmawan/Temp/ecis_test.js",
}, "/user/ariefdarmawan/inbox/temp/", 0, nil)
if es != nil {
	for k, v := range es {
		t.Error(fmt.Sprintf("Error when write %v : %v \n", k, v))
	}
}
```

### Read HDFS Status
```
hdata, _ := h.List("/user/ariefdarmawan")
fmt.Printf("Data:\n%v\n", hdata.FileStatuses.FileStatus)
```
