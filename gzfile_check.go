package main

import (
	"compress/gzip"
	"flag"
	"io"
	"io/ioutil"
	"log"
	"oss"
)

func CheckGzFile(fileName string, ossClient *oss.Client, bucket string) ([]string, error) {
	marker := ""
	errFiles := make([]string, 0)
	for {
		rst, err := ossClient.ListBucket(bucket, marker, fileName, "/")
		if err != nil {
			log.Printf("err;%s \n", err)
			return nil, nil
		}
		marker = rst.NextMarker
		if err != nil {
			log.Printf("err!:%s \n", err)
			return nil, err
		}
		for _, o := range rst.Objects {
			if err := func() error {
				_, readerCloser, err := ossClient.GetObject(bucket, o.Key)
				gzReader, err := gzip.NewReader(readerCloser)
				defer readerCloser.Close()
				defer gzReader.Close()
				if err != nil {
					log.Printf("new gzreader failed !%s \n", err)
					return err
				}
				if err != nil {
					log.Printf("GetObject failed !%s \n", err)
					return err
				}
				written, err := io.Copy(ioutil.Discard, gzReader)
				if err != nil {
					log.Printf("Have found one error file !so we mark it!written is %d \n", written)
					errFiles = append(errFiles, o.Key)
					return err
				}
				return nil
			}(); err != nil {
				log.Printf("oss get and checked failed!err:%s o is %s\n", err, o)
			}
		}
		if marker == "" {
			break
		}
	}
	return errFiles, nil
}

func main() {
	var fileName = flag.String("filepath", "", "oss file path")
	var endpoint = flag.String("endpoint", "oss-cn-hangzhou-internal.aliyuncs.com", "oss endpoint")
	flag.Parse()
	if *fileName == "" {
		log.Printf("file name can not be null \n")
		return
	}
	cfg := &oss.Config{Endpoint: *endpoint,
		Key: "", Secret: ""}
	client, _ := oss.NewClient(cfg)
	errFiles, err := CheckGzFile(*fileName, client, "frio-tegong")
	for i := 0; i < 5 && i < len(errFiles); i++ {
		err = client.DeleteObject("frio-tegong", errFiles[i])
		if err != nil {
			log.Printf("delete files failed ! errFiles:%s err:%s\n", errFiles, err)
			return
		}
	}
	if err != nil {
		log.Printf("err:%s \n", err)
		return
	}
	log.Printf("errFiles is %s \n", errFiles)
}
