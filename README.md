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

### Read HDFS Status
```
hdata, _ := h.List("/user/ariefdarmawan")
fmt.Printf("Data:\n%v\n", hdata.FileStatuses.FileStatus)
```
